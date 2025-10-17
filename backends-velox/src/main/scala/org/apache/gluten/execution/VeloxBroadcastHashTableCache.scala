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

import java.lang.ref.SoftReference
import java.util.concurrent.ConcurrentHashMap

private[execution] object VeloxBroadcastHashTableCache {
  private val cache = new ConcurrentHashMap[Long, SoftReference[Array[Array[Byte]]]]()

  def getOrBuild(hashTableId: Long)(build: => Array[Array[Byte]]): Array[Array[Byte]] = {
    val cached = Option(cache.get(hashTableId)).flatMap(ref => Option(ref.get()))
    cached.getOrElse {
      val computed = build
      if (computed != null) {
        cache.put(hashTableId, new SoftReference(computed))
      }
      computed
    }
  }

  def invalidate(hashTableId: Long): Unit = {
    cache.remove(hashTableId)
  }
}
