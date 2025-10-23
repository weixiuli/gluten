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
package org.apache.gluten.vectorized;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class VeloxHashTableJniWrapper implements RuntimeAware {
  public interface Factory {
    VeloxHashTableJniWrapper create(Runtime runtime);
  }

  private static final Factory DEFAULT_FACTORY = VeloxHashTableJniWrapper::new;
  private static final AtomicReference<Factory> FACTORY = new AtomicReference<>(DEFAULT_FACTORY);

  private final Runtime runtime;

  protected VeloxHashTableJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static VeloxHashTableJniWrapper create(Runtime runtime) {
    return FACTORY.get().create(runtime);
  }

  public static void setFactory(Factory factory) {
    FACTORY.set(Objects.requireNonNullElse(factory, DEFAULT_FACTORY));
  }

  public static void resetFactory() {
    FACTORY.set(DEFAULT_FACTORY);
  }

  public native byte[] build(
      long[] batchHandles,
      int[] keyChannels,
      int joinType,
      boolean nullAware,
      boolean hasFilter,
      int minTableSizeForParallelJoinBuild,
      byte[] schemaBytes);

  public native void registerSerialized(String id, byte[] serialized);

  public native void drop(String id);

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }
}
