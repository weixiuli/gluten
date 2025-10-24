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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  BoundReference,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{IdentitySafeBroadcastMode, VeloxHashTableBuildSideRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

class VeloxBroadcastHashJoinBuilderSuite extends AnyFunSuite {

  private def withConf[T](settings: (String, String)*)(f: => T): T = {
    val sqlConf = new SQLConf
    settings.foreach { case (k, v) => sqlConf.setConfString(k, v) }
    SQLConf.withExistingConf(sqlConf)(f)
  }

  private def newRelation(): VeloxHashTableBuildSideRelation = {
    val buildAttr = AttributeReference("k", IntegerType, nullable = false)()
    VeloxHashTableBuildSideRelation(Seq(buildAttr), Array.empty, IdentitySafeBroadcastMode)
  }

  private val cacheEnabledSettings = Seq(
    GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_ENABLED.key -> "true",
    GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true",
    VeloxConfig.VELOX_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true"
  )

  test("buildIfNeeded short-circuits when cache disabled") {
    val relation = newRelation()
    val context = VeloxBroadCastHashJoinContext(
      buildHashTableId = "test",
      keyOrdinals = Seq(0),
      joinType = Inner,
      buildSide = BuildSide.BuildRight,
      isNullAwareAntiJoin = false,
      hasFilter = false)

    val built = withConf()(VeloxBroadcastHashTableBuilder.buildIfNeeded(relation, context))
    assert(!built)
  }

  test("buildIfNeeded skips non-right build sides") {
    val relation = newRelation()
    val context = VeloxBroadCastHashJoinContext(
      buildHashTableId = "test",
      keyOrdinals = Seq(0),
      joinType = Inner,
      buildSide = BuildSide.BuildLeft,
      isNullAwareAntiJoin = false,
      hasFilter = false)

    val built = withConf(cacheEnabledSettings: _*) {
      VeloxBroadcastHashTableBuilder.buildIfNeeded(relation, context)
    }
    assert(!built)
  }

  test("buildIfNeeded reuses existing serialized tables") {
    val relation = newRelation()
    relation.registerVeloxHashTable("test", Array[Byte](1, 2, 3))
    val context = VeloxBroadCastHashJoinContext(
      buildHashTableId = "test",
      keyOrdinals = Seq(0),
      joinType = Inner,
      buildSide = BuildSide.BuildRight,
      isNullAwareAntiJoin = false,
      hasFilter = false)

    val built = withConf(cacheEnabledSettings: _*) {
      VeloxBroadcastHashTableBuilder.buildIfNeeded(relation, context)
    }
    assert(built)
  }

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType, nullable = false)()

  test("computeKeyOrdinals maps attribute references to ordinals") {
    val buildOutput = Seq(attr("a"), attr("b"))
    val keyExprs: Seq[Expression] = Seq(buildOutput.head)

    val ordinals = VeloxBroadcastHashTableBuilder.computeKeyOrdinals(keyExprs, buildOutput)
    assert(ordinals.contains(Seq(0)))
  }

  test("computeKeyOrdinals handles bound references and rejects unsupported expressions") {
    val buildOutput = Seq(attr("a"))

    val boundOrdinal = VeloxBroadcastHashTableBuilder.computeKeyOrdinals(
      Seq(BoundReference(0, IntegerType, nullable = false)),
      buildOutput)
    assert(boundOrdinal.contains(Seq(0)))

    val unsupported = VeloxBroadcastHashTableBuilder.computeKeyOrdinals(Seq(Literal(1)), buildOutput)
    assert(unsupported.isEmpty)
  }
}
