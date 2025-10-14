# Driver-side hash table serialization for the Velox backend

This guide outlines the steps required to build a broadcast hash table on the
Spark driver when using the Velox backend, serialize it once, and reuse the
serialized payload across executors.  The workflow relies on the Velox fork at
[`codex/add-serialization-and-deserialization-for-hashtable-f73b2u`][velox-branch],
which introduces serialization hooks for Velox hash tables.

[velox-branch]: https://github.com/weixiuli/velox/tree/codex/add-serialization-and-deserialization-for-hashtable-f73b2u

## 1. Prepare the environment

1. Clone the Velox fork that exposes hash-table serialization and make sure the
   Gluten build pulls it in.  If you are building the bundled Velox inside the
   Gluten tree, point `VELOX_HOME` to a checkout of the branch above before
   running the `build_velox.sh` helper.
2. Build Gluten as usual.  The native build will pick up the patched Velox and
   export the additional JNI methods required to (de)serialize hash tables.

## 2. Enable driver-side hash table materialisation

Configure Spark/Gluten so that the driver performs the hash build step and
emits the serialized table:

* Set `spark.gluten.velox.broadcastHashTable.serializeOnDriver` to `true`.  This
  flag gates the new code path inside Gluten that requests the serialized hash
  table from Velox instead of letting each executor build it locally.
* Keep the broadcast size thresholds in check (`spark.sql.autoBroadcastJoinThreshold`
  and `spark.gluten.maxBroadcastTableSize`) to ensure the driver can materialise
  the hash table safely.

When the flag is enabled, the `ColumnarBroadcastExchangeExec` node still
collects the build-side columnar batches, but before handing them to Spark’s
broadcast mechanism it invokes the Velox JNI bridge to:

1. Check at runtime that the native Velox build actually exposed the
   serialization entry points (the helper falls back to executor builds if the
   JNI method is missing).
2. Build the hash table inside the driver process.
3. Serialize the table into an opaque byte array by calling the new Velox API.
4. Wrap the serialized payload inside a dedicated `VeloxUnsafeHashRelation`
   that exposes a fake columnar batch carrying the table identifier so the
   downstream `InputHashTableTransformer` can operate on an `RDD[ColumnarBatch]`
   as usual.

```scala
// backends-velox/src/main/scala/org/apache/gluten/execution/VeloxDriverHashTableSupport.scala
private def buildHashTableOnDriver(
    output: Seq[Attribute],
    mode: HashedRelationBroadcastMode,
    serializedBatches: Array[ColumnarBatchSerializeResult],
    hashTableId: String): Option[VeloxHashTablePayload] = {
  val bytes = withRuntime("buildHashTableOnDriver") { (runtime, wrapper) =>
    wrapper.buildHashTable(
      runtime.getHandle,
      cSchema.memoryAddress(),
      serializedPayload,
      keyOrdinals.get,
      mode.isNullAware,
      hashTableId)
  }
  // Wrap the opaque payload with metadata so Spark can broadcast it.
  Some(VeloxHashTablePayload(bytes, rowCount))
}
```

## 3. Executor-side deserialization

Executors receive the broadcast payload containing the serialized hash table.
Each task retrieves the serialized bytes and calls back into the Velox JNI layer
(depending on the new branch) to

1. Instantiate the Velox hash table from the byte stream.
2. Wire the resulting pointer into the native join operator before the probe
   phase starts.

Because the table is serialized on the driver, the executors skip the expensive
hash-build step, which both reduces redundant work and removes repeated
allocations in executor processes.

```scala
// backends-velox/src/main/scala/org/apache/gluten/execution/VeloxBroadcastBuildSideRDD.scala
val relation = broadcasted.value.asReadOnlyCopy()
val batches = relation match {
  case hashed: VeloxUnsafeHashRelation =>
    VeloxDriverHashTableSupport.ensureHashTableRegistered(hashed)
    VeloxDriverHashTableSupport
      .fakeBroadcastBatch(hashed)
      .map(Iterator.single)
      .getOrElse(hashed.delegate.deserialized)
  case _ =>
    relation.deserialized
}
Iterators.wrap(batches)

// gluten-arrow/src/main/java/org/apache/gluten/vectorized/VeloxHashTableJniWrapper.java
public void registerSerialized(long runtimeHandle, String tableId, byte[] payload, long rowCount) {
  registerSerializedNative(runtimeHandle, tableId, payload, rowCount);
}

// cpp/velox/jni/VeloxJniWrapper.cc
facebook::velox::exec::HashTableSerializer::registerSerialized(
    veloxRuntime->memoryManager()->getLeafMemoryPool().get(),
    hashTableId,
    std::move(buffer),
    static_cast<uint64_t>(rowCount));
```

## 4. Resource lifecycle management

The serialized hash tables follow the same lifecycle as existing broadcast
payloads:

* Gluten tracks the mapping between Spark execution IDs and hash table
  identifiers via the `GlutenDriverEndpoint`.  When a query finishes (or when
  cached data expires), the driver sends `GlutenCleanExecutionResource` messages
  so that executors can drop the native hash tables.
* Make sure the native side implements the corresponding cleanup logic for the
  new serialized format; the Velox fork linked above already contains helper
  APIs for freeing the resources returned by deserialization.

## 5. Validation tips

To verify the workflow:

1. Run a broadcast-hash-heavy benchmark (for example, TPC-H Q5).  Inspect the
   driver logs to confirm that the hash table is serialized once (look for
   Velox logging that mentions serialization/deserialization hooks).
2. On executors, check that the hash table is deserialized exactly once per
   broadcast identifier and that subsequent tasks reuse the cached pointer.
3. Monitor native memory usage: compared to the default behaviour, the executor
   processes should show a lower peak usage because the build step is skipped.

When the native backend is compiled without hash-table serialization support the
driver automatically falls back to the executor-side build path, so enabling the
flag is safe even while you roll out the patched Velox build. Following these
steps allows you to rely on the driver to materialise and serialize the Velox
hash table once, while executors only perform lightweight deserialization before
probing.
