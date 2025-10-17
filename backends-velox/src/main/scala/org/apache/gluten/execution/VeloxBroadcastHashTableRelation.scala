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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

import scala.collection.Seq

/**
 * A thin wrapper that associates serialized Velox hash table bytes with the broadcast relation
 * that already carries the fallback columnar batches.
 */
case class VeloxBroadcastHashTableRelation(
    relation: BuildSideRelation,
    output: Seq[Attribute],
    hashTableId: Long,
    serializedHashTable: Array[Byte])
  extends BuildSideRelation
  with KnownSizeEstimation {

  override def deserialized: Iterator[ColumnarBatch] = relation.deserialized

  override def transform(key: Expression): Array[InternalRow] = relation.transform(key)

  override def asReadOnlyCopy(): BuildSideRelation =
    VeloxBroadcastHashTableRelation(
      relation.asReadOnlyCopy(),
      output,
      hashTableId,
      serializedHashTable)

  override def mode: BroadcastMode = relation.mode

  override def estimatedSize: Long = relation match {
    case known: KnownSizeEstimation => known.estimatedSize
    case _ => 0L
  }
}
