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

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  BoundReference,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.physical.{
  BroadcastMode,
  HashedRelationBroadcastMode,
  IdentityBroadcastMode
}
import org.apache.spark.sql.execution.VeloxHashTableBuildSideRelation
import org.apache.spark.sql.types.IntegerType

import org.scalatest.funsuite.AnyFunSuite

class VeloxBroadcastHashJoinBuilderSuite extends AnyFunSuite {

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType, nullable = false)()

  private def hashedMode(attrs: Seq[AttributeReference]): BroadcastMode = {
    HashedRelationBroadcastMode(attrs.map(_.toAttribute), isNullAware = false)
  }

  test("VeloxBroadcastHashTableMode unwraps embedded context") {
    val output = Seq(attr("a"))
    val context = Some(
      VeloxBroadCastHashJoinContext(
        buildHashTableId = "id",
        keyOrdinals = Seq(0),
        joinType = org.apache.spark.sql.catalyst.plans.Inner,
        buildSide = BuildSide.BuildRight,
        isNullAwareAntiJoin = false,
        hasFilter = false))

    val mode = VeloxBroadcastHashTableMode(hashedMode(output), context)
    val (underlying, extracted) = VeloxBroadcastHashTableMode.unwrap(mode)

    assert(underlying.isInstanceOf[HashedRelationBroadcastMode])
    assert(extracted == context)
  }

  test("VeloxHashTableBuildSideRelation stores hash table payloads") {
    val output = Seq(attr("a"))
    val payload = Array[Byte](1, 2, 3)
    val relation = VeloxHashTableBuildSideRelation(output, payload, IdentityBroadcastMode)

    assert(relation.hashTables.sameElements(payload))
    val snapshot = relation.hashTables
    snapshot(0) = 42
    assert(!relation.hashTables.sameElements(snapshot))
    val cloned = relation.asReadOnlyCopy()
    assert(cloned ne relation)
    assert(cloned.hashTables.sameElements(payload))
  }

  test("VeloxHashTableBuildSideRelation tracks optional hash join context") {
    val output = Seq(attr("a"))
    val payload = Array[Byte](9, 8, 7)
    val context = VeloxBroadCastHashJoinContext(
      buildHashTableId = "test",
      keyOrdinals = Seq(0),
      joinType = org.apache.spark.sql.catalyst.plans.Inner,
      buildSide = BuildSide.BuildRight,
      isNullAwareAntiJoin = false,
      hasFilter = false)

    val relation =
      VeloxHashTableBuildSideRelation(output, payload, IdentityBroadcastMode, Some(context))
    assert(relation.context.contains(context))

    val copy = relation.asReadOnlyCopy()
    assert(copy.context.contains(context))

    val updated = relation.withContext(None)
    assert(updated.context.isEmpty)
    assert(updated.hashTables.sameElements(payload))
  }

  test("buildSerializedHashTables skips unsupported broadcast modes") {
    val output = Seq(attr("a"))
    val relation = VeloxBroadcastHashTableBuilder
      .buildSerializedHashTables(output, Array(Array.emptyByteArray), IdentityBroadcastMode, None)
    assert(relation.isEmpty)
  }

  test("buildSerializedHashTables requires valid key ordinals") {
    val output = Seq(attr("a"))
    val mode = hashedMode(output)
    val relation = VeloxBroadcastHashTableBuilder
      .buildSerializedHashTables(output, Array.empty[Array[Byte]], mode, None)
    assert(relation.isEmpty)
  }

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
