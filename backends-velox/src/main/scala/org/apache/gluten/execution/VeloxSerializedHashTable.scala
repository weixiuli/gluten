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
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.VeloxHashTableJniWrapper

import org.apache.spark.task.TaskResources

/** Metadata describing a serialized Velox hash table. */
case class VeloxSerializedHashTable(
    namedStruct: Array[Byte],
    numKeys: Int,
    ignoreNullKeys: Boolean,
    allowDuplicates: Boolean,
    isJoinBuild: Boolean,
    hasProbedFlag: Boolean,
    minTableSizeForParallelJoinBuild: Int,
    serializedRows: Array[Byte])
    extends Serializable {

  def deserializeHandle(): Long = TaskResources.runUnsafe {
    val runtime =
      Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "VeloxSerializedHashTable#deserialize")
    val jniWrapper = VeloxHashTableJniWrapper.create(runtime)
    jniWrapper.deserialize(
      serializedRows,
      namedStruct,
      numKeys,
      ignoreNullKeys,
      allowDuplicates,
      isJoinBuild,
      hasProbedFlag,
      minTableSizeForParallelJoinBuild)
  }

  def closeHandle(handle: Long): Unit = TaskResources.runUnsafe {
    val runtime =
      Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "VeloxSerializedHashTable#close")
    VeloxHashTableJniWrapper.create(runtime).close(handle)
  }
}
