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

import java.util.Arrays

import org.apache.gluten.execution.VeloxBroadCastHashJoinContext

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, BindReferences, Expression}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{BroadcastModeUtils, SafeBroadcastMode}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

/**
 * Velox specific build side relation that carries serialized hash table payloads for broadcast hash
 * joins.
 */
class VeloxHashTableBuildSideRelation private (
    val output: Seq[Attribute],
    private val serializedHashTables: Array[Byte],
    private val safeBroadcastMode: SafeBroadcastMode,
    private val contextOpt: Option[VeloxBroadCastHashJoinContext])
  extends BuildSideRelation
  with KnownSizeEstimation
  with Serializable {

  private val payload = VeloxHashTableBuildSideRelation.normalize(serializedHashTables)

  override def mode: BroadcastMode = BroadcastModeUtils.fromSafe(safeBroadcastMode, output)

  /** Serialized Velox hash table bytes. */
  def hashTables: Array[Byte] = payload.clone()

  /** Optional broadcast hash join context associated with the payload. */
  def context: Option[VeloxBroadCastHashJoinContext] = contextOpt

  override def deserialized: Iterator[ColumnarBatch] = Iterator.empty

  override def transform(key: Expression): Array[InternalRow] = Array.empty[InternalRow]

  override def asReadOnlyCopy(): VeloxHashTableBuildSideRelation =
    new VeloxHashTableBuildSideRelation(output, payload.clone(), safeBroadcastMode, contextOpt)

  def withContext(
      context: Option[VeloxBroadCastHashJoinContext]): VeloxHashTableBuildSideRelation =
    new VeloxHashTableBuildSideRelation(output, payload.clone(), safeBroadcastMode, context)

  override def estimatedSize: Long = payload.length

  override def equals(obj: Any): Boolean = obj match {
    case other: VeloxHashTableBuildSideRelation =>
      output == other.output &&
      safeBroadcastMode == other.safeBroadcastMode &&
      Arrays.equals(payload, other.payload) &&
      contextOpt == other.contextOpt
    case _ => false
  }

  override def hashCode(): Int = {
    var result = output.hashCode()
    result = 31 * result + safeBroadcastMode.hashCode()
    result = 31 * result + Arrays.hashCode(payload)
    result = 31 * result + contextOpt.hashCode()
    result
  }
}

object VeloxHashTableBuildSideRelation {
  private def normalize(bytes: Array[Byte]): Array[Byte] = {
    if (bytes == null || bytes.isEmpty) { Array.emptyByteArray } else { bytes.clone() }
  }

  private def toSafeMode(output: Seq[Attribute], mode: BroadcastMode): SafeBroadcastMode = {
    val boundMode = mode match {
      case HashedRelationBroadcastMode(keys, isNullAware) =>
        val boundKeys =
          keys.map(k => BindReferences.bindReference(k, AttributeSeq(output)))
        HashedRelationBroadcastMode(boundKeys, isNullAware)
      case other => other
    }
    BroadcastModeUtils.toSafe(boundMode)
  }

  def apply(
      output: Seq[Attribute],
      hashTables: Array[Byte],
      mode: BroadcastMode,
      context: Option[VeloxBroadCastHashJoinContext] = None): VeloxHashTableBuildSideRelation = {
    new VeloxHashTableBuildSideRelation(output, hashTables, toSafeMode(output, mode), context)
  }

  def apply(
      output: Seq[Attribute],
      hashTables: Array[Byte],
      mode: SafeBroadcastMode,
      context: Option[VeloxBroadCastHashJoinContext] = None): VeloxHashTableBuildSideRelation = {
    new VeloxHashTableBuildSideRelation(output, hashTables, mode, context)
  }
}
