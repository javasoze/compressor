package com.senseidb.compressor.idset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LongArrayIdSet extends IdSet {

  public static class LongArrayIdIterator implements LongRandomAccessIterator {
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
  
  public static void serialize(LongArrayIdSet idset,OutputStream o) throws IOException{
    DataOutputStream out = new DataOutputStream(o);
    out.writeInt(idset.vals.length);
    for (int i=0;i<idset.vals.length;++i){
      out.writeLong(idset.vals[i]);
    }
    out.writeInt(idset.currentIdx);
  }
  
  public static LongArrayIdSet deserialize(InputStream input) throws IOException{
    DataInputStream in = new DataInputStream(input);
    int count = in.readInt();
    LongArrayIdSet idSet = new LongArrayIdSet(count);
    for (int i=0;i<count;++i){
      idSet.vals[i] = in.readLong();
    }
    idSet.currentIdx = in.readInt();
    return idSet;
  }
}
