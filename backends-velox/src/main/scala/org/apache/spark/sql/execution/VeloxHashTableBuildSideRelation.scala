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
package org.apache.spark.sql.execution

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

/**
 * Velox specific build side relation that augments [[ColumnarBuildSideRelation]] with
 * serialized hash table storage for broadcast hash joins.
 */
class VeloxHashTableBuildSideRelation private (
    private val columnar: ColumnarBuildSideRelation)
  extends BuildSideRelation
  with KnownSizeEstimation
  with Serializable {

  private val veloxHashTables = mutable.HashMap.empty[String, Array[Byte]]

  def registerVeloxHashTable(hashTableId: String, serialized: Array[Byte]): Unit = synchronized {
    veloxHashTables.update(hashTableId, serialized)
  }

  def getVeloxHashTable(hashTableId: String): Option[Array[Byte]] = synchronized {
    veloxHashTables.get(hashTableId)
  }

  def hasVeloxHashTable(hashTableId: String): Boolean = synchronized {
    veloxHashTables.contains(hashTableId)
  }

  def asColumnar: ColumnarBuildSideRelation = columnar

  override def mode: BroadcastMode = columnar.mode

  override def deserialized: Iterator[ColumnarBatch] = columnar.deserialized

  override def transform(key: Expression): Array[InternalRow] = columnar.transform(key)

  override def asReadOnlyCopy(): VeloxHashTableBuildSideRelation = this

  override def estimatedSize: Long = columnar.estimatedSize

  override def equals(obj: Any): Boolean = obj match {
    case other: VeloxHashTableBuildSideRelation => columnar == other.columnar
    case _ => false
  }

  override def hashCode(): Int = columnar.hashCode()
}

object VeloxHashTableBuildSideRelation {
  def apply(columnar: ColumnarBuildSideRelation): VeloxHashTableBuildSideRelation =
    new VeloxHashTableBuildSideRelation(columnar)

  def apply(
      output: Seq[Attribute],
      batches: Array[Array[Byte]],
      mode: BroadcastMode): VeloxHashTableBuildSideRelation =
    apply(ColumnarBuildSideRelation(output, batches, mode))
}
