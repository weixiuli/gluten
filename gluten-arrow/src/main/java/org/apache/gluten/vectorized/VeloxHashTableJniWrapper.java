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
package org.apache.gluten.vectorized;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

/**
 * JNI bridge that exposes Velox hash-table serialization utilities required for broadcasting driver
 * materialised hash tables.
 */
public class VeloxHashTableJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private VeloxHashTableJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static VeloxHashTableJniWrapper create(Runtime runtime) {
    return new VeloxHashTableJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public byte[] buildHashTable(
      long runtimeHandle,
      long arrowSchemaAddress,
      byte[][] serializedBatches,
      int[] keyOrdinals,
      boolean nullAware,
      String tableId) {
    return buildHashTableNative(
        runtimeHandle, arrowSchemaAddress, serializedBatches, keyOrdinals, nullAware, tableId);
  }

  public boolean isHashTableSerializationSupported(long runtimeHandle) {
    return isHashTableSerializationSupportedNative(runtimeHandle);
  }

  public void registerSerialized(
      long runtimeHandle, String tableId, byte[] payload, long rowCount) {
    registerSerializedNative(runtimeHandle, tableId, payload, rowCount);
  }

  public void releaseSerialized(long runtimeHandle, String tableId) {
    releaseSerializedNative(runtimeHandle, tableId);
  }

  private native byte[] buildHashTableNative(
      long runtimeHandle,
      long arrowSchemaAddress,
      byte[][] serializedBatches,
      int[] keyOrdinals,
      boolean nullAware,
      String tableId);

  private native boolean isHashTableSerializationSupportedNative(long runtimeHandle);

  private native void registerSerializedNative(
      long runtimeHandle, String tableId, byte[] payload, long rowCount);

  private native void releaseSerializedNative(long runtimeHandle, String tableId);
}
