package com.senseidb.compressor.util;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;

import com.senseidb.compressor.io.ByteBufferInputStream;
import com.senseidb.compressor.io.ByteBufferOutputStream;
import com.senseidb.compressor.io.DirectByteBufferDataInput;
import com.senseidb.compressor.io.DirectByteBufferDataOutput;

public class CompressorUtil {
  public static int getNumBits(long val) {
    int count = 0;
    while (val > 0) {
      count++;
      val = val >> 1;
    }
    return count;
  }
  
  public static DataOutput getDataOutput(ByteBuffer buffer){
    if (buffer.isDirect()){
      return new DirectByteBufferDataOutput(buffer);
    }
    else{
      return new OutputStreamDataOutput(new ByteBufferOutputStream(buffer));
    }
  }
  
  public static DataInput getDataInput(ByteBuffer buffer){
    if (buffer.isDirect()){
      return new DirectByteBufferDataInput(buffer);
    }
    else{
      return new InputStreamDataInput(new ByteBufferInputStream(buffer));
    }
  }
  
  public static <T> Iterator<T> iterator(final T[] arr){
    return new Iterator<T>(){
      int count = 0;
      @Override
      public boolean hasNext() {
        return count < arr.length;
      }

      @Override
      public T next() {
        return arr[count++];
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove not supported");
      }
      
    };
  }
  
  public static void main(String[] args) {
    ByteBuffer buf = ByteBuffer.allocate(16);
    
    System.out.println(buf.capacity());
    
    LongBuffer lbuf = buf.asLongBuffer();
    System.out.println(lbuf.capacity());
    
  }
}
