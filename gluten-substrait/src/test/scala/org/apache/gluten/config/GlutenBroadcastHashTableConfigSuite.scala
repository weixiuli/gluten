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

class GlutenBroadcastHashTableConfigSuite extends AnyFunSuiteLike {

  test("broadcast hash table master switch defaults") {
    val sqlConf = new SQLConf
    val config = new GlutenConfig(sqlConf)

    assert(config.enableBroadcastHashTable)
    assert(!config.enableBroadcastHashTableCache)
  }

  test("broadcast hash table cache requires master switch") {
    val sqlConf = new SQLConf
    sqlConf.setConfString(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_ENABLED.key,
      "false")
    sqlConf.setConfString(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_CACHE_ENABLED.key,
      "true")

    val config = new GlutenConfig(sqlConf)

    assert(!config.enableBroadcastHashTable)
    assert(!config.enableBroadcastHashTableCache)
  }

  test("broadcast hash table cache activates when all switches enabled") {
    val sqlConf = new SQLConf
    sqlConf.setConfString(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_ENABLED.key,
      "true")
    sqlConf.setConfString(
      GlutenConfig.COLUMNAR_BROADCAST_HASH_TABLE_CACHE_ENABLED.key,
      "true")

    val config = new GlutenConfig(sqlConf)

    assert(config.enableBroadcastHashTable)
    assert(config.enableBroadcastHashTableCache)
  }
}
