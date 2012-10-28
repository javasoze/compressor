package com.senseidb.compressor.idset;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

public interface ForwardIndex {
  void add(int idx, long val);
  long get(int idx);
  long sizeInBytes();
  void save(DataOutput output) throws IOException;
  ReaderIterator iterator(DataInput input) throws IOException;
  Reader load(DataInput input) throws IOException;
}
