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
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.VeloxHashTableBuilderJniWrapper

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  AttributeSeq,
  BindReferences,
  BoundReference,
  Expression
}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.optimizer.BuildSide.BuildRight
import org.apache.spark.sql.catalyst.plans.{
  ExistenceJoin,
  Inner,
  InnerLike,
  JoinType,
  LeftAnti,
  LeftOuter,
  LeftSemi,
  RightOuter
}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.{ColumnarBuildSideRelation, VeloxHashTableBuildSideRelation}
import org.apache.spark.task.TaskResources

case class VeloxBroadCastHashJoinContext(
    buildHashTableId: String,
    keyOrdinals: Seq[Int],
    joinType: JoinType,
    buildSide: BuildSide,
    isNullAwareAntiJoin: Boolean,
    hasFilter: Boolean)

object VeloxBroadcastHashTableBuilder {

  def buildSerializedHashTables(
      output: Seq[Attribute],
      serializedBatches: Array[Array[Byte]],
      mode: BroadcastMode,
      context: Option[VeloxBroadCastHashJoinContext]): Option[VeloxHashTableBuildSideRelation] =
    synchronized {
      mode match {
        case hashedMode: HashedRelationBroadcastMode =>
          val boundKeys =
            hashedMode.key.map(k => BindReferences.bindReference(k, AttributeSeq(output)))
          val ordinalsOpt = computeKeyOrdinals(boundKeys, output)
          if (ordinalsOpt.isEmpty || serializedBatches.isEmpty) {
            None
          } else {
            val relation = ColumnarBuildSideRelation(output, serializedBatches, mode)
            buildSerializedHashTables(
              relation,
              ordinalsOpt.get,
              Inner,
              BuildRight,
              hashedMode.isNullAware,
              hasFilter = false)
              .map(bytes => VeloxHashTableBuildSideRelation(output, bytes, mode, context))
          }
        case _ => None
      }
    }

  private def buildSerializedHashTables(
      relation: ColumnarBuildSideRelation,
      keyOrdinals: Seq[Int],
      joinType: JoinType,
      buildSide: BuildSide,
      isNullAwareAntiJoin: Boolean,
      hasFilter: Boolean): Option[Array[Byte]] = {
    if (buildSide != BuildRight || keyOrdinals.isEmpty) {
      return None
    }

    val veloxJoinTypeOrdinalOpt = toVeloxJoinTypeOrdinal(joinType)
    if (veloxJoinTypeOrdinalOpt.isEmpty) {
      return None
    }

    TaskResources.runUnsafe {
      val runtime = Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "VeloxBroadcastHashTableBuilder#buildSerializedHashTables")
      val builderWrapper = VeloxHashTableBuilderJniWrapper.create(runtime)
      val keyChannels = keyOrdinals.map(_.toInt).toArray
      val iterator = relation.deserialized

      var builderHandle = 0L
      var hasInput = false
      var serialized: Array[Byte] = Array.emptyByteArray

      try {
        while (iterator.hasNext) {
          val batch = iterator.next()
          try {
            val veloxBatch = VeloxColumnarBatches.ensureVeloxBatch(batch)
            val handle =
              ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, veloxBatch)
            if (!hasInput) {
              builderHandle = builderWrapper.create(
                handle,
                keyChannels,
                veloxJoinTypeOrdinalOpt.get,
                isNullAwareAntiJoin,
                hasFilter)
              hasInput = true
            }
            builderWrapper.addInput(builderHandle, handle)
          } finally {
            batch.close()
          }
        }

        if (!hasInput) {
          None
        } else {
          serialized = builderWrapper.serialize(builderHandle)
          Some(serialized)
        }
      } finally {
        if (builderHandle != 0L) {
          builderWrapper.close(builderHandle)
        }
      }
    }
  }

  def computeKeyOrdinals(
      buildKeyExprs: Seq[Expression],
      buildOutput: Seq[Attribute]): Option[Seq[Int]] = {
    val ordinals = buildKeyExprs.map {
      case attr: AttributeReference =>
        buildOutput.indexWhere(_.exprId == attr.exprId)
      case bound: BoundReference =>
        bound.ordinal
      case _ =>
        -1
    }
    if (ordinals.contains(-1)) {
      None
    } else {
      Some(ordinals)
    }
  }

  private def toVeloxJoinTypeOrdinal(joinType: JoinType): Option[Int] = joinType match {
    case _: InnerLike => Some(0)
    case LeftOuter => Some(1)
    case RightOuter => Some(2)
    case org.apache.spark.sql.catalyst.plans.FullOuter => Some(3)
    case LeftSemi => Some(4)
    case ExistenceJoin(_) => Some(5)
    case LeftAnti => Some(8)
    case _ => None
  }
}
