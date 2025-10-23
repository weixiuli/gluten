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

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.unsafe.{UnsafeBytesBufferArray, UnsafeColumnarBuildSideRelation}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

import java.util.Arrays

class VeloxHashTableBuildSideRelationSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "200M")
      .set("spark.memory.offHeap.enabled", "true")
  }

  private var delegate: UnsafeColumnarBuildSideRelation = _
  private val serializedHashTable: Array[Byte] = Array[Byte](1, 2, 3, 4)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val attr = AttributeReference("a", StringType, nullable = false, metadata = null)()
    val output = Seq(attr)
    val bytesArray = UnsafeBytesBufferArray(1, Array(4), 4)
    bytesArray.putBytesBuffer(0, Array[Byte](10, 20, 30, 40))
    delegate =
      UnsafeColumnarBuildSideRelation(output, bytesArray, IdentityBroadcastMode)
  }

  test("Java serialization preserves Velox hash table metadata") {
    val relation = VeloxHashTableBuildSideRelation(delegate, "table-1", serializedHashTable)
    val serializer = new JavaSerializer(sparkContext.conf).newInstance()
    val buffer = serializer.serialize(relation)
    val restored = serializer.deserialize[VeloxHashTableBuildSideRelation](buffer)

    assert(restored.buildHashTableId === "table-1")
    assert(Arrays.equals(restored.serializedHashTable, serializedHashTable))
    assert(restored.mode == delegate.mode)
  }

  test("Kryo serialization preserves Velox hash table metadata") {
    val relation = VeloxHashTableBuildSideRelation(delegate, "table-2", serializedHashTable)
    val serializer = new KryoSerializer(sparkContext.conf).newInstance()
    val buffer = serializer.serialize(relation)
    val restored = serializer.deserialize[VeloxHashTableBuildSideRelation](buffer)

    assert(restored.buildHashTableId === "table-2")
    assert(Arrays.equals(restored.serializedHashTable, serializedHashTable))
    assert(restored.mode == delegate.mode)
  }

  test("asReadOnlyCopy clones delegate and preserves metadata") {
    val relation = VeloxHashTableBuildSideRelation(delegate, "table-3", serializedHashTable)
    val copy = relation.asReadOnlyCopy().asInstanceOf[VeloxHashTableBuildSideRelation]

    assert(copy ne relation)
    assert(copy.delegate ne relation.delegate)
    assert(copy.buildHashTableId === relation.buildHashTableId)
    assert(Arrays.equals(copy.serializedHashTable, relation.serializedHashTable))
    assert(copy.mode == relation.mode)
  }
}
