package com.senseidb.compressor.perf;

import java.util.Arrays;
import java.util.Random;

import com.senseidb.compressor.idset.CompressedIdSet;
import com.senseidb.compressor.idset.LongArrayIdSet;
import com.senseidb.compressor.idset.IdSet.LongRandomAccessIterator;

public class CompressIdPerf {

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int count =  1024 * 1024; // 100M longs
    // int maxVal = 1000000;
    Random rand = new Random();
    LongArrayIdSet set2 = new LongArrayIdSet(count);
    long s1, e1;

    s1 = System.currentTimeMillis();
    for (int i = 0; i < count; ++i) {
      long v = Math.abs(rand.nextInt());
      set2.addID(v);
    }
    e1 = System.currentTimeMillis();
    System.out.println(" uncompressed size: " + set2.sizeInBytes() + ", took: "
        + (e1 - s1));
    
    int blockSize = 1024;
    long[] data = set2.vals;
    long[] copy = new long[data.length];
    System.arraycopy(data, 0, copy, 0, data.length);

    Arrays.sort(copy);


    CompressedIdSet set = new CompressedIdSet(blockSize);
    s1 = System.currentTimeMillis();
    for (long v : copy) {
      set.addID(v);
    }
    e1 = System.currentTimeMillis();
    System.out.println("compressed size: " + set.sizeInBytes() + ", took: "
        + (e1 - s1));
    
    LongRandomAccessIterator iter1 = set.iterator();
    LongRandomAccessIterator iter2 = set2.iterator();

    long start, end;

    start = System.currentTimeMillis();
    long tmp = 0;
    while (iter1.hasNext()) {
      long v = iter1.next();
      tmp += v;
    }
    end = System.currentTimeMillis();
    System.out.println("compressed time1: " + (end - start));

    start = System.currentTimeMillis();
    while (iter2.hasNext()) {
      long v = iter2.next();
      tmp += v;
    }
    end = System.currentTimeMillis();
    System.out.println("uncompresed time2: " + (end - start));

    start = System.currentTimeMillis();
    for (int i = 0; i < iter1.numElems(); ++i) {
      long v = iter1.get(i);
      tmp += v;
    }
    end = System.currentTimeMillis();
    System.out.println("compressed ra time1: " + (end - start));

    start = System.currentTimeMillis();
    for (int i = 0; i < iter2.numElems(); ++i) {
      long v = iter2.get(i);
      tmp += v;
    }
    end = System.currentTimeMillis();
    System.out.println("uncompressed ra time2: " + (end - start));
    
    start = System.currentTimeMillis();
    int numElems = (int)iter2.numElems();
    for (int i = 0; i < numElems; ++i) {
      int val = rand.nextInt(numElems);
      long v = iter2.get(val);
      tmp += v;
    }
    end = System.currentTimeMillis();
    System.out.println("random compressed ra time2: " + (end - start));
    
    start = System.currentTimeMillis();

    numElems = (int)iter2.numElems();
    for (int i = 0; i < numElems; ++i) {
      int val = rand.nextInt(numElems);
      long v = iter2.get(val);
      tmp += v;
    }
    end = System.currentTimeMillis();
    System.out.println("random uncompressed ra time2: " + (end - start));

  }

}
