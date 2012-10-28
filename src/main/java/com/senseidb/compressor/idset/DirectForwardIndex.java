package com.senseidb.compressor.idset;

import java.io.IOException;
import java.nio.LongBuffer;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.PackedInts.Reader;
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
  
  private static long[] readFromDataInput(DataInput in) throws IOException{
    int count = in.readInt();
    long[] arr = new long[count];
    for (int i=0;i<count;++i){
      arr[i] = in.readLong();
    }
    return arr;
  }

  @Override
  public ReaderIterator iterator(DataInput in) throws IOException{
    long[] arr = readFromDataInput(in);
    final LongBuffer buf = LongBuffer.wrap(arr);
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
  public void save(DataOutput buf) throws IOException {
    buf.writeInt(arr.length);
    for (int i=0;i<arr.length;++i){
      buf.writeLong(arr[i]);
    }
  }

  @Override
  public Reader load(DataInput input) throws IOException {
    final long[] arr = readFromDataInput(input);
    return new Reader(){

      @Override
      public long get(int index) {
        return arr[index];
      }

      @Override
      public int get(int index, long[] target, int off, int len) {
        int minLen = Math.min(arr.length, len);
        System.arraycopy(arr, index,target, off, minLen);
        return minLen;
      }

      @Override
      public int getBitsPerValue() {
        return 64;
      }

      @Override
      public int size() {
        return arr.length;
      }

      @Override
      public long ramBytesUsed() {
        return 8*arr.length;
      }

      @Override
      public Object getArray() {
        return arr;
      }

      @Override
      public boolean hasArray() {
        return true;
      }
      
    };
  }
  
  
}
