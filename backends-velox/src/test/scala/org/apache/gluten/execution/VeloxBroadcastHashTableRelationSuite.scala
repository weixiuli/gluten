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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

import org.scalatest.funsuite.AnyFunSuite

class VeloxBroadcastHashTableRelationSuite extends AnyFunSuite {

  private val output = Seq(AttributeReference("col", IntegerType)())

  test("asReadOnlyCopy clones underlying relation and preserves metadata") {
    val base = new RecordingRelation(estimated = 128L)
    val serialized = Array[Byte](1, 2, 3)
    val relation =
      VeloxBroadcastHashTableRelation(base, output, hashTableId = 7L, serializedHashTable = serialized)

    val copy = relation.asReadOnlyCopy().asInstanceOf[VeloxBroadcastHashTableRelation]

    assert(base.copyInvocations === 1)
    assert(copy ne relation)
    assert(copy.hashTableId === 7L)
    assert(copy.serializedHashTable eq serialized)
    assert(copy.relation.isInstanceOf[RecordingRelation])
    assert(copy.relation ne base)
  }

  test("delegates to wrapped relation for deserialization, transform, and mode") {
    val base = new RecordingRelation(estimated = 64L)
    val relation = VeloxBroadcastHashTableRelation(base, output, 5L, Array.emptyByteArray)

    relation.deserialized
    relation.transform(Literal(1))

    assert(base.deserializedInvocations === 1)
    assert(base.transformInvocations === 1)
    assert(relation.mode eq DummyBroadcastMode)
  }

  test("estimatedSize delegates to KnownSizeEstimation when available") {
    val base = new RecordingRelation(estimated = 256L)
    val relation = VeloxBroadcastHashTableRelation(base, output, 3L, Array(42.toByte))

    assert(relation.estimatedSize === 256L)
  }

  test("estimatedSize falls back to zero when relation size is unknown") {
    val base = new UnknownSizeRelation
    val relation = VeloxBroadcastHashTableRelation(base, output, 11L, Array(1.toByte))

    assert(relation.estimatedSize === 0L)
  }

  private class RecordingRelation(estimated: Long)
      extends BuildSideRelation
      with KnownSizeEstimation {
    var copyInvocations: Int = 0
    var deserializedInvocations: Int = 0
    var transformInvocations: Int = 0

    override def deserialized: Iterator[ColumnarBatch] = {
      deserializedInvocations += 1
      Iterator.empty
    }

    override def transform(key: Expression): Array[InternalRow] = {
      transformInvocations += 1
      Array.empty[InternalRow]
    }

    override def asReadOnlyCopy(): BuildSideRelation = {
      copyInvocations += 1
      new RecordingRelation(estimated)
    }

    override def mode: BroadcastMode = DummyBroadcastMode

    override def estimatedSize: Long = estimated
  }

  private class UnknownSizeRelation extends BuildSideRelation {
    override def deserialized: Iterator[ColumnarBatch] = Iterator.empty

    override def transform(key: Expression): Array[InternalRow] = Array.empty[InternalRow]

    override def asReadOnlyCopy(): BuildSideRelation = new UnknownSizeRelation

    override def mode: BroadcastMode = DummyBroadcastMode
  }

  private object DummyBroadcastMode extends BroadcastMode {
    override def transform(rows: Iterator[InternalRow]): Array[InternalRow] = rows.toArray
  }
}
