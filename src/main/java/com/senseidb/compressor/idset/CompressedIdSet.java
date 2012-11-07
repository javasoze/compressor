package com.senseidb.compressor.idset;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Mutable;

import com.senseidb.compressor.util.CompressorUtil;

public class CompressedIdSet extends IdSet {
  static class ValSeg {
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
}
