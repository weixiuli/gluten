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
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{VeloxHashTableJniWrapper, ColumnarBatchSerializeResult}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil

import org.apache.arrow.c.ArrowSchema

object VeloxHashTableBuilder {
  def build(
      output: Seq[Attribute],
      mode: BroadcastMode,
      serialized: Array[ColumnarBatchSerializeResult]): Option[Array[Byte]] = {
    if (serialized.isEmpty) {
      return None
    }

    val runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName, "BuildSideRelation#hashTable")
    val wrapper = VeloxHashTableJniWrapper.create(runtime)

    val allocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(allocator)
    try {
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)

      val batchBytes = serialized.flatMap(_.getSerialized)
      if (batchBytes.isEmpty) {
        return None
      }

      val numKeys = mode match {
        case hashed: HashedRelationBroadcastMode => hashed.key.length
        case _ => 0
      }
      val ignoreNullKeys = mode match {
        case hashed: HashedRelationBroadcastMode => !hashed.isNullAware
        case _ => true
      }

      val bytes = wrapper.buildSerializedHashTable(
        cSchema.memoryAddress(),
        batchBytes,
        numKeys,
        ignoreNullKeys,
        true, // allow duplicates for joins
        true, // is join build
        false, // has probed flag
        0)
      if (bytes != null && bytes.nonEmpty) {
        Some(bytes)
      } else {
        None
      }
    } finally {
      cSchema.close()
    }
  }
}
