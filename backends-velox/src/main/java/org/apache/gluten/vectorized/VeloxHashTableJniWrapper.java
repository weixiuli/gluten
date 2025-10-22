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

/** JNI wrapper that exposes Velox hash table serialization utilities. */
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

  public native byte[] build(
      byte[][] batches,
      byte[] namedStruct,
      int numKeys,
      boolean ignoreNullKeys,
      boolean allowDuplicates,
      boolean isJoinBuild,
      boolean hasProbedFlag,
      int minTableSizeForParallelJoinBuild);

  public native long deserialize(
      byte[] serializedRows,
      byte[] namedStruct,
      int numKeys,
      boolean ignoreNullKeys,
      boolean allowDuplicates,
      boolean isJoinBuild,
      boolean hasProbedFlag,
      int minTableSizeForParallelJoinBuild);

  public native void close(long handle);

  public native void registerBroadcastHashTable(String id, long handle);

  public native void unregisterBroadcastHashTable(String id);
}
