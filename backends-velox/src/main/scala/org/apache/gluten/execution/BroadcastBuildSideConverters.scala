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
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.ColumnarBatchSerializerJniWrapper

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.ArrowSchema

object BroadcastBuildSideConverters {
  def columnarBatchIteratorFromBytes(
      runtimeContext: String,
      output: Seq[Attribute],
      batchBytes: Array[Array[Byte]]): Iterator[ColumnarBatch] = {
    val runtime =
      Runtimes.contextInstance(BackendsApiManager.getBackendName, runtimeContext)
    val allocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(allocator)
    var schemaClosed = false

    try {
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)

      val jniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
      var serializeHandle: Long = -1L

      try {
        serializeHandle = jniWrapper.init(cSchema.memoryAddress())
      } catch {
        case t: Throwable =>
          if (!schemaClosed) {
            cSchema.close()
            schemaClosed = true
          }
          throw t
      }

      Iterators
        .wrap(new Iterator[ColumnarBatch] {
          private var batchId = 0

          override def hasNext: Boolean = batchId < batchBytes.length

          override def next: ColumnarBatch = {
            val handle = jniWrapper.deserialize(serializeHandle, batchBytes(batchId))
            batchId += 1
            ColumnarBatches.create(handle)
          }
        })
        .protectInvocationFlow()
        .recycleIterator {
          if (serializeHandle != -1L) {
            jniWrapper.close(serializeHandle)
          }
          if (!schemaClosed) {
            cSchema.close()
            schemaClosed = true
          }
        }
        .recyclePayload(ColumnarBatches.forceClose)
        .create()
    } catch {
      case t: Throwable =>
        if (!schemaClosed) {
          cSchema.close()
          schemaClosed = true
        }
        throw t
    }
  }
}
