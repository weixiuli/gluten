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
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized.VeloxHashTableBuilderJniWrapper

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.optimizer.BuildSide.BuildRight
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.ColumnarBuildSideRelation
import org.apache.spark.task.TaskResources

case class VeloxBroadCastHashJoinContext(
    buildHashTableId: String,
    keyOrdinals: Seq[Int],
    joinType: JoinType,
    buildSide: BuildSide,
    isNullAwareAntiJoin: Boolean,
    hasFilter: Boolean)

object VeloxBroadcastHashTableBuilder {

  def buildIfNeeded(
      relation: ColumnarBuildSideRelation,
      context: VeloxBroadCastHashJoinContext): Boolean = synchronized {
    if (!VeloxConfig.get.enableBroadcastHashTableCache) {
      return false
    }
    if (relation.hasVeloxHashTable(context.buildHashTableId)) {
      return true
    }
    if (context.buildSide != BuildRight) {
      return false
    }
    if (context.keyOrdinals.isEmpty) {
      return false
    }

    val veloxJoinTypeOrdinalOpt = toVeloxJoinTypeOrdinal(context.joinType)
    if (veloxJoinTypeOrdinalOpt.isEmpty) {
      return false
    }

    TaskResources.runUnsafe {
      val runtime = Runtimes.contextInstance(
        BackendsApiManager.getBackendName,
        "VeloxBroadcastHashTableBuilder#buildIfNeeded")
      val builderWrapper = VeloxHashTableBuilderJniWrapper.create(runtime)
      val keyChannels = context.keyOrdinals.map(_.toInt).toArray
      val iterator = relation.deserialized

      var builderHandle = 0L
      var hasInput = false
      var buildSucceeded = false

      try {
        while (iterator.hasNext) {
          val batch = iterator.next()
          val veloxBatch = VeloxColumnarBatches.ensureVeloxBatch(batch)
          val handle =
            ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, veloxBatch)
          if (!hasInput) {
            builderHandle = builderWrapper.create(
              handle,
              keyChannels,
              veloxJoinTypeOrdinalOpt.get,
              context.isNullAwareAntiJoin,
              context.hasFilter)
            hasInput = true
          }
          builderWrapper.addInput(builderHandle, handle)
        }

        if (!hasInput) {
          buildSucceeded = false
        } else {
          val serialized = builderWrapper.serialize(builderHandle)
          relation.registerVeloxHashTable(context.buildHashTableId, serialized)
          buildSucceeded = true
        }
      } finally {
        if (builderHandle != 0L) {
          builderWrapper.close(builderHandle)
        }
      }

      buildSucceeded
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
