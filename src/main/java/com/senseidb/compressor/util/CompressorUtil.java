package com.senseidb.compressor.util;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class CompressorUtil {
  public static int getNumBits(long val) {
    int count = 0;
    while (val > 0) {
      count++;
      val = val >> 1;
    }
    return count;
  }
  
  public static void main(String[] args) {
    ByteBuffer buf = ByteBuffer.allocate(16);
    
    System.out.println(buf.capacity());
    
    LongBuffer lbuf = buf.asLongBuffer();
    System.out.println(lbuf.capacity());
    
  }
}
