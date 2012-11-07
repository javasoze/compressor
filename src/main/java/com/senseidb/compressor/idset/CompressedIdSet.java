package com.senseidb.compressor.idset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Mutable;

import com.senseidb.compressor.util.CompressorUtil;


public class CompressedIdSet extends IdSet {
  private static class ValSeg {
    long minVal;
    Mutable valSet;

    ValSeg() {
    }

    public long sizeInBytes() {
      return 8 + 4 + valSet.getBitsPerValue()*valSet.size();
    }
    
    public static void serialize(ValSeg idset,DataOutputStream out) throws IOException{
      out.writeLong(idset.minVal);
      out.writeInt(idset.valSet.size());
      out.writeInt(idset.valSet.getBitsPerValue());
      int count = idset.valSet.size();
      out.writeInt(count);
      for (int i=0;i<count;++i){
        out.writeLong(idset.valSet.get(i));
      }
    }
    
    public static ValSeg deserialize(DataInputStream in) throws IOException{
      long minVal = in.readLong();
      int valCount = in.readInt();
      int bitsPerVal = in.readInt();
      int len = in.readInt();
      
      Mutable valSet = PackedInts.getMutable(valCount, bitsPerVal,PackedInts.DEFAULT);
      
      for (int i=0;i<len;++i){
        valSet.set(i, in.readLong());
      }
      
      ValSeg seg = new ValSeg();
      seg.minVal = minVal;
      seg.valSet = valSet;
      return seg;
    }
  }

  private static class CompressedLongIterator implements
      LongRandomAccessIterator {

    private Iterator<ValSeg> iter;
    private ValSeg cs = null;
    private int readCursor;
    private long lastVal;

    private final long[] currentSeg;
    private final int currentCount;
    private final ArrayList<ValSeg> segList;
    private final int size;
    
    private int tmpSegIdx = -1;
    private long[] tmpSeg = null;

    CompressedLongIterator(long[] currentSeg, int currentCount,
        LinkedList<ValSeg> segList, int size) {
      this.currentCount = currentCount;
      this.currentSeg = currentSeg;
      this.segList = new ArrayList<ValSeg>(segList);
      this.size = size;
      reset();
    }

    @Override
    public long get(int idx) {
      assert idx >= 0 && idx < size;
      int i = idx / currentSeg.length;
      int m = idx % currentSeg.length;

      if (i >= segList.size()) {
        // last block
        return currentSeg[m];
      } else {
        if (tmpSegIdx != i || tmpSeg==null){
          tmpSegIdx = i;
          ValSeg seg = this.segList.get(i);
          tmpSeg = new long[seg.valSet.size()];
          tmpSeg[0] = seg.minVal;
          for (int k=1;k<tmpSeg.length;++k){
            tmpSeg[k] = tmpSeg[k-1]+seg.valSet.get(k);
          }
        }
        return tmpSeg[m];
      }
    }

    @Override
    public boolean hasNext() {
      if (cs == null) {
        return readCursor < currentCount;
      }
      return true;
    }

    @Override
    public long next() {
      if (cs == null) {
        long val = currentSeg[readCursor];
        readCursor++;
        return val;
      }
      if (readCursor < cs.valSet.size()) {
        long val = cs.valSet.get(readCursor);
        val += lastVal;
        lastVal = val;
        if (readCursor == cs.valSet.size() - 1) {
          readCursor = 0;
          if (iter.hasNext()) {
            cs = iter.next();
            lastVal = cs.minVal;
          } else {
            cs = null;
            lastVal = 0;
          }
        } else {
          readCursor++;
        }
        return val;
      }
      return -1;
    }

    @Override
    public void reset() {
      iter = segList.iterator();
      readCursor = 0;
      if (iter.hasNext()) {
        cs = iter.next();
        lastVal = cs.minVal;
      } else {
        cs = null;
        lastVal = 0;
      }
    }

    @Override
    public long numElems() {
      return size;
    }
  }

  private long maxDelta;
  private final long[] currentSeg;
  private int currentCount;
  private int size = 0;
  private final LinkedList<ValSeg> segList = new LinkedList<ValSeg>();
  
  
  public static void serialize(CompressedIdSet idset,OutputStream output) throws IOException{
    DataOutputStream out = new DataOutputStream(output);
    int blockSize = idset.currentSeg.length;
    out.writeInt(blockSize);
    for (int i=0;i<blockSize;++i){
      out.writeLong(idset.currentSeg[i]);
    }
    out.writeLong(idset.maxDelta);
    out.writeInt(idset.currentCount);
    out.writeInt(idset.size);
    out.writeInt(idset.segList.size());
    for (ValSeg seg : idset.segList){
      ValSeg.serialize(seg, out);
    }
  }
  
  public static CompressedIdSet deserialize(InputStream input) throws IOException{
    DataInputStream in = new DataInputStream(input);
    int blockSize = in.readInt();
    
    CompressedIdSet idSet = new CompressedIdSet(blockSize);
    long[] currentSeg = idSet.currentSeg;
    for (int i=0;i<blockSize;++i){
      currentSeg[i] = in.readLong();
    }
    idSet.maxDelta = in.readLong();
    idSet.currentCount = in.readInt();
    idSet.size = in.readInt();
    int segLen = in.readInt();

    for (int i=0;i<segLen;++i){
      ValSeg seg = ValSeg.deserialize(in);
      idSet.segList.add(seg);
    }
    
    return idSet;
  }

  public CompressedIdSet(int blockSize) {
    currentSeg = new long[blockSize];
    init();
  }

  public long sizeInBytes() {
    long size = currentCount * 8;
    for (ValSeg seg : segList) {
      size += seg.sizeInBytes();
    }
    return size;
  }

  void init() {
    currentCount = 0;
    maxDelta = -1;
  }

  @Override
  public void addID(long val) {
    if (currentCount == 0) {
      currentSeg[currentCount++] = val;
    } else {
      long delta = val - currentSeg[currentCount];
      if (maxDelta < delta) {
        maxDelta = delta;
      }
      currentSeg[currentCount++] = val;
    }
    if (currentCount == currentSeg.length) {
      compressBlock();
    }
    size++;
  }

  private void compressBlock() {
    int nBits = CompressorUtil.getNumBits(maxDelta);
    ValSeg seg = new ValSeg();
    seg.minVal = currentSeg[0];
    seg.valSet = PackedInts.getMutable(currentSeg.length, nBits, PackedInts.DEFAULT);
    for (int i = 0; i < currentSeg.length; ++i) {
      if (i > 0) {
        long val = currentSeg[i] - currentSeg[i - 1];
        seg.valSet.set(i, val);
      } else {
        seg.valSet.set(i, 0);
      }
    }
    segList.add(seg);
    init();
  }

  @Override
  public LongRandomAccessIterator iterator() {
    return new CompressedLongIterator(currentSeg, currentCount, segList, size);
  }

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
