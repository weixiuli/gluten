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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Placeholder column vector that carries the serialized Velox hash table payload alongside the
 * broadcast relation. Consumers can downcast to retrieve the table identifier, payload bytes, and
 * the build-side row count while treating the batch as a regular columnar payload in Spark's
 * broadcast pipeline.
 */
public class VeloxHashTableColumnVector extends ColumnVector {
  private final String hashTableId;
  private final byte[] payload;
  private final long rowCount;

  public VeloxHashTableColumnVector(String hashTableId, byte[] payload, long rowCount) {
    super(DataTypes.BinaryType);
    this.hashTableId = hashTableId;
    this.payload = payload;
    this.rowCount = rowCount;
  }

  public String hashTableId() {
    return hashTableId;
  }

  public byte[] payload() {
    return payload;
  }

  public long rowCount() {
    return rowCount;
  }

  @Override
  public void close() {
    // No resources to free. The payload is managed by the broadcast relation lifecycle.
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    if (rowId != 0) {
      throw new IndexOutOfBoundsException("Hash table vector exposes a single logical row");
    }
    return rowCount;
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (rowId != 0) {
      throw new IndexOutOfBoundsException("Hash table vector exposes a single logical row");
    }
    return UTF8String.fromString(hashTableId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (rowId != 0) {
      throw new IndexOutOfBoundsException("Hash table vector exposes a single logical row");
    }
    return payload;
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException();
  }
}
