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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.vectorized.VeloxHashTableJniWrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.VeloxHashTableBuildSideRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.scalatest.Assertions.fail

import scala.collection.mutable.ArrayBuffer

class VeloxBroadcastBuildSideRDDSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.enabled", "true")
  }

  private class DummyRelation extends BuildSideRelation {
    override def deserialized: Iterator[ColumnarBatch] = Iterator.empty

    override def transform(key: Expression): Array[InternalRow] = Array.empty

    override def asReadOnlyCopy(): BuildSideRelation = this

    override def mode: IdentityBroadcastMode.type = IdentityBroadcastMode
  }

  test("Velox broadcast build side registers serialized hash tables") {
    val registrations = ArrayBuffer.empty[(String, Array[Byte])]
    val drops = ArrayBuffer.empty[String]
    VeloxHashTableJniWrapper.setFactory(runtime =>
      new VeloxHashTableJniWrapper(runtime) {
        override def build(
            batchHandles: Array[Long],
            keyChannels: Array[Int],
            joinType: Int,
            nullAware: Boolean,
            hasFilter: Boolean,
            minTableSizeForParallelJoinBuild: Int,
            schemaBytes: Array[Byte]): Array[Byte] = {
          throw new UnsupportedOperationException("build should not be called in this test")
        }

        override def registerSerialized(id: String, serialized: Array[Byte]): Unit = {
          registrations += id -> serialized.clone()
        }

        override def drop(id: String): Unit = {
          drops += id
        }
      })

    try {
      val serialized = Array[Byte](5, 4, 3, 2)
      val broadcastRelation =
        VeloxHashTableBuildSideRelation(new DummyRelation, "hash-table", serialized)
      val broadcast = spark.sparkContext.broadcast(broadcastRelation: BuildSideRelation)
      val iterator = VeloxBroadcastBuildSideRDD(spark.sparkContext, broadcast)
        .genBroadcastBuildSideIterator()

      val veloxIterator = iterator.asInstanceOf[VeloxBroadcastBuildSideIterator]
      assert(veloxIterator.buildHashTableId.contains("hash-table"))
      assert(registrations.map(_._1) == Seq("hash-table"))
      assert(registrations.head._2.toSeq == serialized.toSeq)
      assert(drops.isEmpty)
    } finally {
      VeloxHashTableJniWrapper.resetFactory()
    }
  }

  test("Velox broadcast iterator exposes empty hash table id when none present") {
    VeloxHashTableJniWrapper.setFactory(runtime =>
      new VeloxHashTableJniWrapper(runtime) {
        override def build(
            batchHandles: Array[Long],
            keyChannels: Array[Int],
            joinType: Int,
            nullAware: Boolean,
            hasFilter: Boolean,
            minTableSizeForParallelJoinBuild: Int,
            schemaBytes: Array[Byte]): Array[Byte] = {
          throw new UnsupportedOperationException()
        }

        override def registerSerialized(id: String, serialized: Array[Byte]): Unit = {
          fail("registerSerialized should not be called for non-hash relations")
        }

        override def drop(id: String): Unit = {
          fail("drop should not be called for non-hash relations")
        }
      })

    try {
      val relation = new DummyRelation
      val broadcast = spark.sparkContext.broadcast(relation: BuildSideRelation)
      val iterator = VeloxBroadcastBuildSideRDD(spark.sparkContext, broadcast)
        .genBroadcastBuildSideIterator()

      val veloxIterator = iterator.asInstanceOf[VeloxBroadcastBuildSideIterator]
      assert(veloxIterator.buildHashTableId.isEmpty)
    } finally {
      VeloxHashTableJniWrapper.resetFactory()
    }
  }

  test("Velox broadcast iterator skips registration when prebuilt hash tables disabled") {
    val registrations = ArrayBuffer.empty[(String, Array[Byte])]
    val drops = ArrayBuffer.empty[String]
    VeloxHashTableJniWrapper.setFactory(runtime =>
      new VeloxHashTableJniWrapper(runtime) {
        override def build(
            batchHandles: Array[Long],
            keyChannels: Array[Int],
            joinType: Int,
            nullAware: Boolean,
            hasFilter: Boolean,
            minTableSizeForParallelJoinBuild: Int,
            schemaBytes: Array[Byte]): Array[Byte] = {
          throw new UnsupportedOperationException("build should not be called in this test")
        }

        override def registerSerialized(id: String, serialized: Array[Byte]): Unit = {
          registrations += id -> serialized.clone()
        }

        override def drop(id: String): Unit = {
          drops += id
        }
      })

    try {
      withSQLConf(VeloxConfig.VELOX_PREBUILT_HASH_TABLE_ENABLED.key -> "false") {
        val serialized = Array[Byte](9, 8, 7)
        val broadcastRelation =
          VeloxHashTableBuildSideRelation(new DummyRelation, "hash-table", serialized)
        val broadcast = spark.sparkContext.broadcast(broadcastRelation: BuildSideRelation)
        val iterator = VeloxBroadcastBuildSideRDD(spark.sparkContext, broadcast)
          .genBroadcastBuildSideIterator()

        val veloxIterator = iterator.asInstanceOf[VeloxBroadcastBuildSideIterator]
        assert(veloxIterator.buildHashTableId.isEmpty)
        assert(registrations.isEmpty)
        assert(drops.isEmpty)
      }
    } finally {
      VeloxHashTableJniWrapper.resetFactory()
    }
  }
}
