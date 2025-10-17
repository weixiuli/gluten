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
import org.apache.gluten.execution.BroadcastBuildSideConverters
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.VeloxHashTableJniWrapper

import org.apache.spark.{broadcast, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.ColumnarBuildSideRelation
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.c.ArrowSchema

import scala.util.control.NonFatal

case class VeloxBroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation])
  extends BroadcastBuildSideRDD(sc, broadcasted)
  with Logging {

  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    val relation = broadcasted.value.asReadOnlyCopy()
    val fromHashTable = relation match {
      case columnar: ColumnarBuildSideRelation =>
        deserializeHashTable(columnar.output, columnar.serializedHashTable)
      case unsafe: UnsafeColumnarBuildSideRelation =>
        deserializeHashTable(unsafe.output, unsafe.serializedHashTable)
      case _ => None
    }

    fromHashTable.getOrElse {
      Iterators
        .wrap(relation.deserialized)
        .recyclePayload(batch => batch.close())
        .create()
    }
  }

  private def deserializeHashTable(
      output: Seq[Attribute],
      serialized: Option[Array[Byte]]): Option[Iterator[ColumnarBatch]] = {
    if (!VeloxConfig.get.enableBroadcastHashTable) {
      return None
    }

    serialized.filter(_.nonEmpty).flatMap { bytes =>
      try {
        val runtime = Runtimes.contextInstance(
          BackendsApiManager.getBackendName,
          "VeloxBroadcastBuildSideRDD#deserializeHashTable")
        val allocator = ArrowBufferAllocators.contextInstance()
        val cSchema = ArrowSchema.allocateNew(allocator)
        try {
          val arrowSchema = SparkArrowUtil.toArrowSchema(
            SparkShimLoader.getSparkShims.structFromAttributes(output),
            SQLConf.get.sessionLocalTimeZone)
          ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)

          val wrapper = VeloxHashTableJniWrapper.create(runtime)
          val handle = wrapper.deserialize(bytes)
          try {
            val batchBytes = Option(wrapper.hashTableToColumnarBatches(handle, cSchema.memoryAddress()))
              .getOrElse(Array.empty[Array[Byte]])
            Some(
              BroadcastBuildSideConverters.columnarBatchIteratorFromBytes(
                "VeloxBroadcastBuildSideRDD#hashTableToColumnarBatches",
                output,
                batchBytes))
          } finally {
            wrapper.close(handle)
          }
        } finally {
          cSchema.close()
        }
      } catch {
        case NonFatal(t) =>
          logWarning("Failed to deserialize broadcast hash table; falling back to columnar batches", t)
          None
      }
    }
  }
}
