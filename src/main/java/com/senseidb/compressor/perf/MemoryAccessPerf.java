package com.senseidb.compressor.perf;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public class MemoryAccessPerf {

  // Used to obtain direct ByteBuffer's base address outside of the JVM heap

  protected final static Field addressField;

  // Used to directly manipulate memory areas outside of the JVM heap

  protected final static Unsafe UNSAFE;

  static {

    try {

      addressField = Buffer.class.getDeclaredField("address");

      addressField.setAccessible(true);

      Field field = Unsafe.class.getDeclaredField("theUnsafe");

      field.setAccessible(true);

      UNSAFE = (Unsafe) field.get(null);

    } catch (IllegalAccessException e) {

      throw new RuntimeException("Cannot initialize Unsafe.", e);

    } catch (NoSuchFieldException e) {

      throw new RuntimeException("Unable to access field.", e);

    }

  }

  public static long getDirectBufferBaseAddress(Buffer buffer) {
    try {
      return (Long) addressField.get(buffer);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
          "Cannot obtain native address of a MappedByteBuffer.");
    }
  }

  public static long getLong(long addr, int idx) {
    return UNSAFE.getLong(addr+idx<<3);
  }
  
  public static byte getByte(long addr, int idx) {
    return UNSAFE.getByte(addr+idx);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    int num = 100000000; //100M
    ByteBuffer buf = ByteBuffer.allocateDirect(num);
    long addr = getDirectBufferBaseAddress(buf);
    for (int i=0;i<num; ++i){
      int val1 = buf.get(i) & 0xff;
      int val2 = getByte(addr,i) & 0xff;
      if (val1 != val2){
        throw new RuntimeException("bug");
      }
    }
    
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
      int val2 = getByte(addr,i) & 0xff;
      tmp+=val2;
    }
    end = System.currentTimeMillis();
    System.out.println("unsafe took: "+(end-start));
    
  }

}
