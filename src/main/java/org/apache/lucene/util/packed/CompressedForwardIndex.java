package org.apache.lucene.util.packed;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.apache.lucene.util.packed.PackedInts.Reader;

import com.senseidb.compressor.idset.DirectForwardIndex;
import com.senseidb.compressor.idset.ForwardIndex;
import com.senseidb.compressor.util.CompressorUtil;

public class CompressedForwardIndex implements ForwardIndex {
  private final Mutable data;

  public CompressedForwardIndex(long numTerms, int numDocs) {
    int bitsPerVal = CompressorUtil.getNumBits(numTerms);
    data = PackedInts.getMutable(numDocs, bitsPerVal, PackedInts.DEFAULT);
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
  
  public void save(DataOutput dataout) throws IOException{
    data.save(dataout);
  }
  
  public void load(DataInput input) throws IOException{
    Reader reader = PackedInts.getReader(input);
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
    int numTerms = 100000;

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

    int numLookups = 1000000;//numElems;// 50000000;
    start = System.currentTimeMillis();
    for (int i = 0; i < numLookups; ++i) {
       int val = rand.nextInt(numElems);

      idx.get(val);
      // int val2 = idx2.readInt(i);
      // if (val != val2) throw new Exception("broken");
    }
    end = System.currentTimeMillis();

    System.out.println("query took: " + (end - start));
  }
}
