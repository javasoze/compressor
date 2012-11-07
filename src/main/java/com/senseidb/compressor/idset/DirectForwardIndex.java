package com.senseidb.compressor.idset;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts.Reader;

import com.senseidb.compressor.idset.IdSet.LongRandomAccessIterator;

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
  public LongRandomAccessIterator iterator(DataInput in) throws IOException{
    final long[] arr = readFromDataInput(in);
    return new LongRandomAccessIterator(){
      int current = 0;

      @Override
      public boolean hasNext() throws IOException {
        return current < arr.length;
      }

      @Override
      public long next() throws IOException {
        return arr[current++];
      }

      @Override
      public void reset() {
        current = 0;
        
      }

      @Override
      public long get(int idx) throws IOException {
        return arr[idx];
      }

      @Override
      public long numElems() {
        return arr.length;
      }

      @Override
      public boolean contains(long val) {
        for (long v : arr){
          if (v == val) return true;
        }
        return false;
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
      public int getBitsPerValue() {
        return 64;
      }

      @Override
      public int size() {
        return arr.length;
      }

      @Override
      public Object getArray() {
        return arr;
      }

      @Override
      public boolean hasArray() {
        return true;
      }

      @Override
      public int get(int index, long[] target, int offset, int len) {
        int count = Math.min(len, arr.length-index);
        System.arraycopy(arr, index, target, offset, count);
        return count;
      }

      @Override
      public long ramBytesUsed() {
        return sizeInBytes();
      }
      
    };
  }
  
  
}
