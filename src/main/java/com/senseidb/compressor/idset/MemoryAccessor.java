package com.senseidb.compressor.idset;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import sun.misc.Unsafe;

public class MemoryAccessor {
//Used to obtain direct ByteBuffer's base address outside of the JVM heap

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
   return UNSAFE.getLong(addr+idx*8);
 }
 
 public static long getLong(long addr) {
   return UNSAFE.getLong(addr);
 }
 
 public static short getShort(long addr, int idx) {
   return UNSAFE.getShort(addr+idx*2);
 }
 
 public static short getShort(long addr) {
   return UNSAFE.getShort(addr);
 }
 

 public static int getByte(long addr, int idx) {
   return UNSAFE.getByte(addr+idx);
 }
 

 public static void putByte(long addr, int idx, byte val) {
   UNSAFE.putByte(addr+idx, val);
 }

 public static int getInt(long addr, int idx) {
   return UNSAFE.getInt(addr+idx*4);
 }
 

 static int swap(int x) {
            return ((x << 24) |
                    ((x & 0x0000ff00) <<  8) |
                    ((x & 0x00ff0000) >>> 8) |
                    (x >>> 24));
  }
 
 public static int getInt(long addr) {
   return UNSAFE.getInt(addr);
 }
 
 public static void putInt(long addr, int idx, int val) {
   UNSAFE.putInt(addr+idx*4, val);
 }
 
 public static void putByte(long addr, byte val) {
   UNSAFE.putByte(addr, val);
 }
 
 public static void putInt(long addr, int val) {
   UNSAFE.putInt(addr, val);
 }
 
 public static void putLong(long addr, int idx, long val) {
   UNSAFE.putLong(addr+idx*8, val);
 }
 
 public static void putLong(long addr, long val) {
   UNSAFE.putLong(addr, val);
 }
 
 public static void fill(long addr, long len, byte val){
   UNSAFE.setMemory(addr, len, val);
 }
}
