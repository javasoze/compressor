package com.senseidb.compressor.idset;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.search.DocIdSetIterator;

import com.senseidb.compressor.idset.CompressedIdSet.ValSeg;

final class CompressedDocIdSetIterator extends DocIdSetIterator {

  private final long[] currentSeg;
  private final int currentCount;
  private final ValSeg[] segList;
  private final long[] minVals;
  private final int size;

  CompressedDocIdSetIterator(long[] currentSeg, int currentCount,
      LinkedList<ValSeg> segList, int size){
    this.currentCount = currentCount;
    this.currentSeg = currentSeg;
    this.segList = segList.toArray(new ValSeg[0]);
    this.size = size;
    this.minVals = new long[this.segList.length];
    for (int i = 0; i < minVals.length; ++i) {
      this.minVals[i] = this.segList[i].minVal;
    }
  }
  
  int docid = -1;
  
  @Override
  public int docID() {
    return docid;
  }

  @Override
  public int nextDoc() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int advance(int target) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

}
