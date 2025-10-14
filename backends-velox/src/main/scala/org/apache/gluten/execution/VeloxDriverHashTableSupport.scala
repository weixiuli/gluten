/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchSerializeResult, VeloxHashTableColumnVector, VeloxHashTableJniWrapper}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.AttributeSeq
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskContext
import org.apache.spark.task.TaskResources

import org.apache.arrow.c.ArrowSchema

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.util.control.NonFatal

/**
 * Container for serialized Velox hash table payload that can be shipped inside the broadcast
 * relation. The payload stores the opaque bytes returned by the native Velox serializer together
 * with the row count for metrics purposes.
 */
case class VeloxHashTablePayload(bytes: Array[Byte], rowCount: Long)

/**
 * Broadcast relation wrapper that augments the regular columnar build-side relation with an
 * optional driver-side serialized Velox hash table.
 */
case class VeloxUnsafeHashRelation(
    delegate: BuildSideRelation,
    hashTableId: String,
    payload: Option[VeloxHashTablePayload])
  extends BuildSideRelation {

  override def output: Seq[Attribute] = delegate.output

  override def mode: BroadcastMode = delegate.mode

  override def deserialized: Iterator[ColumnarBatch] = delegate.deserialized

  override def asReadOnlyCopy(): VeloxUnsafeHashRelation =
    VeloxUnsafeHashRelation(delegate.asReadOnlyCopy(), hashTableId, payload)

  override def transform(key: Expression): Array[org.apache.spark.sql.catalyst.InternalRow] =
    delegate.transform(key)
}

object VeloxDriverHashTableSupport extends Logging {

  private val registeredTables = new ConcurrentHashMap[String, AtomicInteger]()

  private def withRuntime[T](tag: String)(f: (org.apache.gluten.runtime.Runtime, VeloxHashTableJniWrapper) => T): T = {
    def run(): T = {
      val runtime =
        Runtimes.contextInstance(BackendsApiManager.getBackendName, s"VeloxDriverHashTableSupport.$tag")
      val wrapper = VeloxHashTableJniWrapper.create(runtime)
      f(runtime, wrapper)
    }

    if (TaskResources.inSparkTask()) {
      run()
    } else {
      TaskResources.runUnsafe {
        run()
      }
    }
  }

  private lazy val serializationSupported: Boolean = {
    try {
      withRuntime("serializationCheck") { (runtime, wrapper) =>
        wrapper.isHashTableSerializationSupported(runtime.getHandle)
      }
    } catch {
      case _: UnsatisfiedLinkError =>
        logInfo(
          "Velox backend does not expose hash-table serialization entry points; falling back to " +
            "executor-side hash table materialisation.")
        false
      case NonFatal(e) =>
        logWarning("Unable to verify Velox hash-table serialization support. Falling back to executor build.", e)
        false
    }
  }

  /**
   * Optionally wrap the broadcast build-side relation with a serialized hash table payload when the
   * Velox backend is configured to materialise the hash table on the driver.
   */
  def maybeWrapSerializedRelation(
      buildPlan: SparkPlan,
      mode: BroadcastMode,
      serializedBatches: Array[ColumnarBatchSerializeResult],
      baseRelation: BuildSideRelation): BuildSideRelation = {
    if (!VeloxConfig.get.broadcastHashTableSerializeOnDriver) {
      return baseRelation
    }

    if (!serializationSupported) {
      logInfo(
        "Driver-side Velox hash-table serialization requested but native backend did not report support. " +
          "Falling back to executor-side hash table build.")
      return baseRelation
    }

    mode match {
      case hashed: HashedRelationBroadcastMode if serializedBatches.nonEmpty =>
        val hashTableId = s"BuiltHashTable-${buildPlan.id}"
        val payload = buildHashTableOnDriver(buildPlan.output, hashed, serializedBatches, hashTableId)
        payload match {
          case Some(value) =>
            VeloxUnsafeHashRelation(baseRelation, hashTableId, Some(value))
          case None =>
            baseRelation
        }
      case _ =>
        baseRelation
    }
  }

  /**
   * Register the serialized hash table with the native side before the probe begins.
   */
  def ensureHashTableRegistered(relation: VeloxUnsafeHashRelation): Unit = {
    relation.payload.foreach { payload =>
      val count = registeredTables.computeIfAbsent(relation.hashTableId, _ => new AtomicInteger(0))
      if (count.incrementAndGet() == 1) {
        registerWithNative(relation.hashTableId, payload)
      }
      Option(TaskContext.get()).foreach { context =>
        context.addTaskCompletionListener[Unit] { _ =>
          release(relation.hashTableId)
        }
      }
    }
  }

  private def release(hashTableId: String): Unit = {
    val counter = registeredTables.get(hashTableId)
    if (counter != null) {
      if (counter.decrementAndGet() == 0) {
        registeredTables.remove(hashTableId)
        withRuntime("release") { (runtime, wrapper) =>
          wrapper.releaseSerialized(runtime.getHandle, hashTableId)
        }
      }
    }
  }

  private def registerWithNative(hashTableId: String, payload: VeloxHashTablePayload): Unit = {
    withRuntime("registerWithNative") { (runtime, wrapper) =>
      wrapper.registerSerialized(runtime.getHandle, hashTableId, payload.bytes, payload.rowCount)
    }
  }

  private def buildFakeColumnarBatch(
      relation: VeloxUnsafeHashRelation,
      payload: VeloxHashTablePayload): ColumnarBatch = {
    val vector = new VeloxHashTableColumnVector(relation.hashTableId, payload.bytes, payload.rowCount)
    val batch = new ColumnarBatch(Array(vector))
    batch.setNumRows(1)
    batch
  }

  def fakeBroadcastBatch(relation: VeloxUnsafeHashRelation): Option[ColumnarBatch] = {
    relation.payload.map(payload => buildFakeColumnarBatch(relation, payload))
  }

  private def buildHashTableOnDriver(
      output: Seq[Attribute],
      mode: HashedRelationBroadcastMode,
      serializedBatches: Array[ColumnarBatchSerializeResult],
      hashTableId: String): Option[VeloxHashTablePayload] = {
    val keyOrdinals = resolveKeyOrdinals(output, mode.key)
    if (keyOrdinals.isEmpty) {
      logDebug(
        s"Skip driver-side hash table materialisation for $hashTableId because " +
          s"join keys could not be resolved to simple ordinal references.")
      return None
    }

    val serializedPayload = serializedBatches.flatMap(_.getSerialized)

    val allocator = org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(allocator)

    try {
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)

      val bytes = withRuntime("buildHashTableOnDriver") { (runtime, wrapper) =>
        wrapper.buildHashTable(
          runtime.getHandle,
          cSchema.memoryAddress(),
          serializedPayload,
          keyOrdinals.get,
          mode.isNullAware,
          hashTableId)
      }

      if (bytes == null || bytes.isEmpty) {
        None
      } else {
        val rowCount = serializedBatches.map(_.getNumRows).sum
        Some(VeloxHashTablePayload(bytes, rowCount))
      }
    } catch {
      case NonFatal(e) =>
        logWarning(
          s"Failed to build Velox hash table on driver for $hashTableId. Falling back to " +
            s"executor-side build.",
          e)
        None
    } finally {
      cSchema.close()
    }
  }

  private def resolveKeyOrdinals(
      output: Seq[Attribute],
      keys: Seq[Expression]): Option[Array[Int]] = {
    val bound = keys.map(expr => BindReferences.bindReference(expr, AttributeSeq(output)))
    val ords = bound.collect {
      case BoundReference(ordinal, _, _) => ordinal
    }
    if (ords.length == keys.length) {
      Some(ords.toArray)
    } else {
      None
    }
  }
}
