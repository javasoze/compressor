package com.senseidb.compressor.perf;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import com.senseidb.compressor.idset.MemoryAccessor;

public class MemoryAccessPerf {

  /**
   * @param args
   */
  public static void main2(String[] args) {
    int num = 100000000; //100M
    ByteBuffer buf = ByteBuffer.allocateDirect(num);
    long addr = MemoryAccessor.getDirectBufferBaseAddress(buf);
    
    long tmp = 0L;
    long start,end;
    
    start = System.currentTimeMillis();
    for (int i=0;i<num; ++i){
      int val1 = buf.get(i) & 0xff;
      tmp+=val1;
    }
    end = System.currentTimeMillis();
    System.out.println("took: "+(end-start));
    

    start = System.currentTimeMillis();
    for (int i=0;i<num; ++i){
      int val2 = MemoryAccessor.getByte(addr,i);
      tmp+=val2;
    }
    end = System.currentTimeMillis();
    System.out.println("unsafe took: "+(end-start));
    
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    int num = 100000000; //100M
    LongBuffer buf = ByteBuffer.allocateDirect(num*8).asLongBuffer();
    long[] arr = new long[num];
    long addr = MemoryAccessor.getDirectBufferBaseAddress(buf);
    
    long tmp = 0L;
    long start,end;
    
    start = System.currentTimeMillis();
    for (int i=0;i<num; ++i){
      long val1 =arr[i];
      tmp+=val1;
    }
    end = System.currentTimeMillis();
    System.out.println("arr took: "+(end-start));
    
    start = System.currentTimeMillis();
    for (int i=0;i<num; ++i){
      long val1 = buf.get(i);
      tmp+=val1;
    }
    end = System.currentTimeMillis();
    System.out.println("took: "+(end-start));
    

    start = System.currentTimeMillis();
    for (int i=0;i<num; ++i){
      long val2 = MemoryAccessor.getLong(addr,i);
      tmp+=val2;
    }
    end = System.currentTimeMillis();
    System.out.println("unsafe took: "+(end-start));
    
  }
  
 

}
