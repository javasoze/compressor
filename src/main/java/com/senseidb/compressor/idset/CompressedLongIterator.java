package com.senseidb.compressor.idset;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import com.senseidb.compressor.idset.CompressedIdSet.ValSeg;
import com.senseidb.compressor.idset.IdSet.LongRandomAccessIterator;
import com.senseidb.compressor.util.CompressorUtil;

final class CompressedLongIterator implements LongRandomAccessIterator {

  private Iterator<ValSeg> iter;
  private ValSeg cs = null;
  private int readCursor;
  private long lastVal;

  private final long[] currentSeg;
  private final int currentCount;
  private final ValSeg[] segList;
  private final long[] minVals;
  private final int size;

  private int tmpSegIdx = -1;
  private long[] tmpSeg = null;

  CompressedLongIterator(long[] currentSeg, int currentCount,
      LinkedList<ValSeg> segList, int size) {
    this.currentCount = currentCount;
    this.currentSeg = currentSeg;
    this.segList = segList.toArray(new ValSeg[0]);
    this.size = size;
    this.minVals = new long[this.segList.length];
    for (int i = 0; i < minVals.length; ++i) {
      this.minVals[i] = this.segList[i].minVal;
    }
    reset();
  }

  public boolean contains(long val) {
    int idx = Arrays.binarySearch(this.minVals, val);
    if (idx < 0) {
      idx = -idx+1;
    }
    if (idx == segList.length){
      return Arrays.binarySearch(currentSeg, val) >= 0;
    }
    else{
      ValSeg seg = segList[idx];
      for (int i = 0; i < seg.valSet.size(); ++i) {
        long v = seg.valSet.get(i);
        if (v == val) {
          return true;
        }
        if (v > val)
          break;
      }
    }
    return false;
  }

  @Override
  public long get(int idx) {
    assert idx >= 0 && idx < size;
    int i = idx / currentSeg.length;
    int m = idx % currentSeg.length;

    if (i >= segList.length) {
      // last block
      return currentSeg[m];
    } else {
      if (tmpSegIdx != i || tmpSeg == null) {
        tmpSegIdx = i;
        ValSeg seg = this.segList[i];
        tmpSeg = new long[seg.valSet.size()];
        tmpSeg[0] = seg.minVal;
        for (int k = 1; k < tmpSeg.length; ++k) {
          tmpSeg[k] = tmpSeg[k - 1] + seg.valSet.get(k);
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
    iter = CompressorUtil.iterator(segList);
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
