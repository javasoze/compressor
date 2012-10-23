// This file has been automatically generated, DO NOT EDIT

package org.apache.lucene.util.packed;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Direct wrapping of 32-bits values to a backing array.
 * 
 * @lucene.internal
 */
final class Direct32 extends PackedInts.MutableImpl {
  final int[] values;

  Direct32(int valueCount) {
    super(valueCount, 32);
    values = new int[valueCount];
  }

  Direct32(DataInput in, int valueCount) throws IOException {
    this(valueCount);
    for (int i = 0; i < valueCount; ++i) {
      values[i] = in.readInt();
    }
    final int mod = valueCount % 2;
    if (mod != 0) {
      for (int i = mod; i < 2; ++i) {
        in.readInt();
      }
    }
  }

  @Override
  public long get(final int index) {
    return values[index] & 0xFFFFFFFFL;
  }

  public void set(final int index, final long value) {
    values[index] = (int) (value);
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.sizeOf(values);
  }

  public void clear() {
    Arrays.fill(values, (int) 0L);
  }

  @Override
  public Object getArray() {
    return values;
  }

  @Override
  public boolean hasArray() {
    return true;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
      arr[o] = values[i] & 0xFFFFFFFFL;
    }
    return gets;
  }

  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + sets; i < end; ++i, ++o) {
      values[i] = (int) arr[o];
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert val == (val & 0xFFFFFFFFL);
    Arrays.fill(values, fromIndex, toIndex, (int) val);
  }
}