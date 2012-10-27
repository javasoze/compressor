package com.senseidb.compressor.idset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

public interface ForwardIndex {
  void add(int idx, long val);
  long get(int idx);
  long sizeInBytes();
  void save(OutputStream output) throws IOException;
  ReaderIterator iterator(ByteBuffer input) throws IOException;
}
