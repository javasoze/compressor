package com.senseidb.compressor.idset;

public class LongArrayIdSet extends IdSet {

  private static class LongArrayIdIterator implements LongRandomAccessIterator {
    private final long[] array;
    private final int currentSize;

    LongArrayIdIterator(long[] array, int currentSize) {
      this.array = array;
      this.currentSize = currentSize;
    }

    private int readIdx = 0;

    @Override
    public boolean hasNext() {
      return readIdx < currentSize;
    }

    @Override
    public long next() {
      return array[readIdx++];
    }

    @Override
    public void reset() {
      readIdx = 0;
    }

    @Override
    public long get(int idx) {
      assert idx >= 0 && idx < currentSize;
      return array[idx];
    }

    @Override
    public long numElems() {
      return currentSize;
    }

  }

  int currentIdx = 0;
  public final long[] vals;

  public LongArrayIdSet(int count) {
    vals = new long[count];
  }

  public long sizeInBytes() {
    return 8 * vals.length + 4;
  }

  @Override
  public void addID(long val) {
    vals[currentIdx++] = val;
  }

  @Override
  public LongRandomAccessIterator iterator() {
    return new LongArrayIdIterator(vals, currentIdx);
  }
}
