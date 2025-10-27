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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuiteLike

class VeloxBroadcastHashTableConfigSuite extends AnyFunSuiteLike {

  private def newConfig(settings: (String, String)*): VeloxConfig = {
    val sqlConf = new SQLConf
    settings.foreach { case (key, value) => sqlConf.setConfString(key, value) }
    new VeloxConfig(sqlConf)
  }

  test("velox broadcast hash table cache disabled by default") {
    val config = newConfig()

    assert(config.enableBroadcastHashTable)
    assert(!config.enableBroadcastHashTableCache)
  }

  test("velox broadcast hash table cache requires master switch") {
    val config = newConfig(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_ENABLED.key -> "false",
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true",
      VeloxConfig.VELOX_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true")

    assert(!config.enableBroadcastHashTable)
    assert(!config.enableBroadcastHashTableCache)
  }

  test("velox broadcast hash table cache requires backend toggle") {
    val config = newConfig(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true",
      VeloxConfig.VELOX_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "false")

    assert(config.enableBroadcastHashTable)
    assert(!config.enableBroadcastHashTableCache)
  }

  test("velox broadcast hash table cache activates when all toggles enabled") {
    val config = newConfig(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_ENABLED.key -> "true",
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true",
      VeloxConfig.VELOX_BROADCAST_HASH_TABLE_CACHE_ENABLED.key -> "true")

    assert(config.enableBroadcastHashTable)
    assert(config.enableBroadcastHashTableCache)
  }
}
