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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class VeloxBroadcastHashTableCacheSuite
    extends AnyFunSuite
    with BeforeAndAfterEach {

  private val hashTableId = 42L
  private val otherHashTableId = hashTableId + 1L

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    VeloxBroadcastHashTableCache.invalidate(hashTableId)
    VeloxBroadcastHashTableCache.invalidate(otherHashTableId)
  }

  override protected def afterEach(): Unit = {
    try {
      VeloxBroadcastHashTableCache.invalidate(hashTableId)
      VeloxBroadcastHashTableCache.invalidate(otherHashTableId)
    } finally {
      super.afterEach()
    }
  }

  test("getOrBuild caches non-null results") {
    var buildCount = 0
    def build(): Array[Array[Byte]] = {
      buildCount += 1
      Array(Array(buildCount.toByte))
    }

    val first = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      build()
    }
    val second = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      build()
    }

    assert(buildCount === 1)
    assert(first eq second)
    assert(first.head.sameElements(Array(1.toByte)))
  }

  test("invalidate removes cached value and rebuilds") {
    var buildCount = 0
    def build(): Array[Array[Byte]] = {
      buildCount += 1
      Array(Array((buildCount * 2).toByte))
    }

    val cached = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      build()
    }
    assert(buildCount === 1)

    VeloxBroadcastHashTableCache.invalidate(hashTableId)

    val rebuilt = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      build()
    }

    assert(buildCount === 2)
    assert(!(cached eq rebuilt))
    assert(rebuilt.head.sameElements(Array(4.toByte)))
  }

  test("null results are not cached") {
    var buildCount = 0
    def build(): Array[Array[Byte]] = {
      buildCount += 1
      null
    }

    val first = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      build()
    }
    val second = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      build()
    }

    assert(first == null && second == null)
    assert(buildCount === 2)
  }

  test("entries are cached independently per hash table id") {
    var firstBuildCount = 0
    def buildFirst(): Array[Array[Byte]] = {
      firstBuildCount += 1
      Array(Array(10.toByte))
    }

    var secondBuildCount = 0
    def buildSecond(): Array[Array[Byte]] = {
      secondBuildCount += 1
      Array(Array(20.toByte))
    }

    val firstA = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      buildFirst()
    }
    val firstB = VeloxBroadcastHashTableCache.getOrBuild(hashTableId) {
      buildFirst()
    }

    val secondA = VeloxBroadcastHashTableCache.getOrBuild(otherHashTableId) {
      buildSecond()
    }
    val secondB = VeloxBroadcastHashTableCache.getOrBuild(otherHashTableId) {
      buildSecond()
    }

    assert(firstBuildCount === 1)
    assert(secondBuildCount === 1)
    assert(firstA eq firstB)
    assert(secondA eq secondB)
    assert(!(firstA eq secondA))
  }
}
