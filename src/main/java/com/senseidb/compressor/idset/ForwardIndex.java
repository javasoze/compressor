package com.senseidb.compressor.idset;

public interface ForwardIndex {
  void add(int idx, long val);
  long get(int idx);
  long sizeInBytes();
}
