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
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.VeloxHashTableJniWrapper

import org.apache.spark.{SparkContext, TaskContext, broadcast}
import org.apache.spark.sql.execution.ColumnarBuildSideRelation
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VeloxBroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation],
    context: Option[VeloxBroadCastHashJoinContext])
  extends BroadcastBuildSideRDD(sc, broadcasted) {

  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    val relation = broadcasted.value.asReadOnlyCopy()
    val veloxConfig = VeloxConfig.get
    context match {
      case Some(hashContext)
          if veloxConfig.enableBroadcastHashTableCache &&
            relation.isInstanceOf[ColumnarBuildSideRelation] &&
            relation
              .asInstanceOf[ColumnarBuildSideRelation]
              .getVeloxHashTable(hashContext.buildHashTableId)
              .isDefined =>
        val serialized = relation
          .asInstanceOf[ColumnarBuildSideRelation]
          .getVeloxHashTable(hashContext.buildHashTableId)
          .get
        val runtime =
          Runtimes.contextInstance(
            BackendsApiManager.getBackendName,
            "VeloxBroadcastBuildSideRDD#genBroadcastBuildSideIterator")
        val jniWrapper = VeloxHashTableJniWrapper.create(runtime)
        val hashTableId = hashContext.buildHashTableId
        jniWrapper.register(hashTableId, serialized)
        Option(TaskContext.get()).foreach { tc =>
          tc.addTaskCompletionListener[Unit] { _ =>
            try {
              jniWrapper.unregister(hashTableId)
            } catch {
              case _: Throwable =>
              // Ignore cleanup exceptions to avoid masking task failures.
            }
          }
        }
        Iterator.empty
      case _ =>
        Iterators
          .wrap(relation.deserialized)
          .recyclePayload(batch => batch.close())
          .create()
    }
  }
}
