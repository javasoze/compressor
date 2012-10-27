package com.senseidb.compressor.idset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

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
  public long sizeInBytes() {
    return 8 * arr.length;
  }

  @Override
  public ReaderIterator iterator(ByteBuffer in) {
    final LongBuffer buf = in.asLongBuffer();
    return new ReaderIterator(){

      @Override
      public void close() throws IOException {
        
      }

      @Override
      public long next() throws IOException {
        return buf.get();
      }

      @Override
      public LongsRef next(int count) throws IOException {
        int size = Math.min(count, buf.remaining());
        
        long[] retBuf = new long[size];
        buf.get(retBuf);
        return new LongsRef(retBuf,0,size);
      }

      @Override
      public int getBitsPerValue() {
        return 64;
      }

      @Override
      public int size() {
        return buf.capacity();
      }

      @Override
      public int ord() {
        return buf.position();
      }
      
    };
  }

  @Override
  public long get(int idx) {
    return arr[idx];
  }

  @Override
  public void save(OutputStream output) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(arr.length * 8);
    LongBuffer longBuf = buf.asLongBuffer();
    longBuf.put(arr);
    output.write(buf.array());
  }
}
