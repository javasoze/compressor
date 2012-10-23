package com.senseidb.compressor.idset;

public class DirectForwardIndex implements ForwardIndex {

  private final long[] arr;

  public DirectForwardIndex(int numdocs) {
    arr = new long[numdocs];
  }

  @Override
  public void add(int idx, long val) {
    assert idx >= 0 && idx < arr.length;
    arr[idx] = val;
  }

  @Override
  public long get(int idx) {
    assert idx >= 0 && idx < arr.length;
    return arr[idx];
  }

  @Override
  public long sizeInBytes() {
    return 8 * arr.length;
  }
}
