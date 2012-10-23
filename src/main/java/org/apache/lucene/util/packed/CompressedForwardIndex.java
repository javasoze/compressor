package org.apache.lucene.util.packed;

import java.util.Random;

import com.senseidb.compressor.idset.DirectForwardIndex;
import com.senseidb.compressor.idset.ForwardIndex;
import com.senseidb.compressor.util.CompressorUtil;

public class CompressedForwardIndex implements ForwardIndex {
  private final Packed64 data;

  public CompressedForwardIndex(long numTerms, int numDocs) {
    int bitsPerVal = CompressorUtil.getNumBits(numTerms);
    data = new Packed64(numDocs, bitsPerVal);
  }

  @Override
  public void add(int idx, long val) {
    data.set(idx, val);
  }

  @Override
  public long get(int idx) {
    return data.get(idx);
  }

  @Override
  public long sizeInBytes() {
    return data.ramBytesUsed();
  }

  static ForwardIndex getNormal(int numTerms, int numDocs) {
    DirectForwardIndex array = new DirectForwardIndex(numDocs);
    System.out.println(array.sizeInBytes());
    return array;
  }

  static ForwardIndex getPacked(int numTerms, int numDocs) {
    CompressedForwardIndex packed = new CompressedForwardIndex(numTerms,
        numDocs);
    System.out.println("bytes used: " + packed.sizeInBytes());
    return packed;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int numElems = 100000000;
    int numTerms = 1000000;

    Random rand = new Random();
    ForwardIndex idx = getPacked(numTerms, numElems);
    // ForwardIntIndex idx2 = getNormal(numTerms,numElems);

    long start = System.currentTimeMillis();
    for (int i = 0; i < numElems; ++i) {
      int val = rand.nextInt(numTerms);
      idx.add(i, val);
      // idx2.addInt(i, val);
    }
    long end = System.currentTimeMillis();

    System.out.println("loading took: " + (end - start));

    int numLookups = numElems;// 50000000;
    start = System.currentTimeMillis();
    for (int i = 0; i < numLookups; ++i) {
      // int val = rand.nextInt(numElems);

      idx.get(i);
      // int val2 = idx2.readInt(i);
      // if (val != val2) throw new Exception("broken");
    }
    end = System.currentTimeMillis();

    System.out.println("query took: " + (end - start));
  }
}