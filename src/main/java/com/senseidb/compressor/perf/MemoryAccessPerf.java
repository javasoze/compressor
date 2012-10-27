package com.senseidb.compressor.perf;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.Random;

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
  public static void main3(String[] args) {
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
      long val2 = MemoryAccessor.getLong(addr,i);
      tmp+=val2;
    }
    end = System.currentTimeMillis();
    System.out.println("unsafe took: "+(end-start));
    
    
    start = System.currentTimeMillis();
    for (int i=0;i<num; ++i){
      long val1 = buf.get(i);
      tmp+=val1;
    }
    end = System.currentTimeMillis();
    System.out.println("took: "+(end-start));
    

    
  }
  
 
  public static void testUnsafePerformance() throws Exception {
        // number of times to run each test
        final int numPasses = 5;
        // How much memory to use for each test case. 2^14 pages..
        final int memorySize = 4096 * (1 << 14);
        // Allocate space
        byte[] bytes = new byte[memorySize];
        int[] ints = new int[memorySize / 4];
        ByteBuffer direct = ByteBuffer.allocateDirect(memorySize);
        long directBaseAddr = MemoryAccessor.getDirectBufferBaseAddress(direct);
        // Arrays used to save results. 0: byte[]  1: int[]  2: Unsafe byte access 3: Unsafe int access
        long[] timings = new long[4];
        // Test 1: fill memory performance
        timings[0] = timings[1] = timings[2] = timings[3] = 0L;
        for (int i = 0; i < numPasses; i++) {
          // fill byte array
          long startNanos = System.nanoTime();
          Arrays.fill(bytes, (byte)0);
          timings[0] += System.nanoTime() - startNanos;
          // fill int array
          startNanos = System.nanoTime();
          Arrays.fill(ints, 0);
          timings[1] += System.nanoTime() - startNanos;
          // fill direct memory
          startNanos = System.nanoTime();
          MemoryAccessor.fill(directBaseAddr, memorySize, (byte)0);
          timings[2] += System.nanoTime() - startNanos;
        }
        System.out.println(String.format("Filling memory area with 0s (averaged over %d runs): \n" +
                                         "  byte array:\t%d nanos\n" +
                                         "  int array:\t%d nanos\n" +
                                         "  unsafe fill:\t%d nanos",
                                         numPasses,
                                         timings[0]/numPasses,
                                         timings[1]/numPasses,
                                         timings[2]/numPasses));
        // Test 2: random memory writes
        final int numAccess = 2000000;
        // Generate 2M indices
        int[] byteIndices = new int[numAccess];
        int[] intIndices = new int[numAccess];
        Random random = new Random(616L);
        for (int i = 0; i < numAccess; i++) {
          byteIndices[i] = Math.abs(random.nextInt()) % memorySize;
          intIndices[i] = byteIndices[i] / 4;
        }
        timings[0] = timings[1] = timings[2] = timings[3] = 0L;
        for (int i = 0; i < numPasses; i++) {
          // Byte array write
          long startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            bytes[byteIndices[j]] = (byte)100;
          }
          timings[0] += System.nanoTime() - startNano;
          // Int array writes
          startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            ints[intIndices[j]] = 100;
          }
          timings[1] += System.nanoTime() - startNano;
          // Unsafe byte writes
          startNano = System.nanoTime();

          for (int j = 0; j < numAccess; j++) {
            MemoryAccessor.putByte(directBaseAddr, byteIndices[j], (byte)100);
          }

          timings[2] += System.nanoTime() - startNano;
          // Unsafe int writes
          startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            // Note that putInt uses absolute address, not int indices
            MemoryAccessor.putInt(directBaseAddr + byteIndices[j], 100);
          }
          timings[3] += System.nanoTime() - startNano;
        }
        System.out.println(String.format("Random writes (averaged over %d runs %d accesses): \n" +
                                         "  byte array:\t%d nanos\n" +
                                         "  int array:\t%d nanos\n" +
                                         "  unsafe byte:\t%d nanos\n" +
                                         "  unsafe int:\t%d nanos",
                                         numPasses,
                                         numAccess,
                                         timings[0] / (numPasses * numAccess),
                                         timings[1] / (numPasses * numAccess),
                                         timings[2] / (numPasses * numAccess),
                                         timings[3] / (numPasses * numAccess)));
        // Test 3: Random memory reads
        timings[0] = timings[1] = timings[2] = timings[3] = 0L;
        long sum = 0L;
        long directIntTime = 0L;
        long directByteTime = 0L;
        for (int i = 0; i < numPasses; i++) {
          // Byte array reads
          long startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            sum += bytes[byteIndices[j]];
          }
          timings[0] += System.nanoTime() - startNano;
          // Int array reads
          startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            sum += ints[intIndices[j]];
          }
          timings[1] += System.nanoTime() - startNano;
          // Unsafe byte reads
          startNano  = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            sum += MemoryAccessor.getByte(directBaseAddr , byteIndices[j]);
          }
          timings[2] += System.nanoTime() - startNano;
          // Unsafe int reads
          startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            // Note that getInt uses absolute address, not int indices
            sum += MemoryAccessor.getInt(directBaseAddr + byteIndices[j]);
          }
          timings[3] += System.nanoTime() - startNano;
          
       // direct int reads
          startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            // Note that getInt uses absolute address, not int indices
            sum += direct.getInt(intIndices[j]);
          }
          directIntTime += System.nanoTime() - startNano;
          
       // direct byte reads
          startNano = System.nanoTime();
          for (int j = 0; j < numAccess; j++) {
            // Note that getInt uses absolute address, not int indices
            sum += direct.get(byteIndices[j]);
          }
          directByteTime += System.nanoTime() - startNano;
        }
        System.out.println(String.format("Random reads (averaged over %d runs %d accesses): \n" +
                                         "  byte array:\t%d nanos\n" +
                                         "  int array:\t%d nanos\n" +
                                         "  unsafe byte:\t%d nanos\n" +
                                         "  unsafe int:\t%d nanos\n" +
                                         "  direct int:\t%d nanos\n" +
                                         "  direct byte:\t%d nanos\n" ,
                                         numPasses,
                                         numAccess,
                                         timings[0] / (numPasses * numAccess),
                                         timings[1] / (numPasses * numAccess),
                                         timings[2] / (numPasses * numAccess),
                                         timings[3] / (numPasses * numAccess),
                                         directIntTime / (numPasses * numAccess),
                                         directByteTime / (numPasses * numAccess)));
        System.out.println("Sum: " + sum);
      }
  
  public static void main(String[] args) throws Exception{
    testUnsafePerformance();
  }

}
