package com.senseidb.compressor.idset;

import java.io.IOException;

public abstract class IdSet {

  public interface LongRandomAccessIterator {
    boolean hasNext() throws IOException;

    long next() throws IOException;

    void reset();

    long get(int idx) throws IOException;
    
    boolean contains(long val);

    long numElems();
  }

  public abstract void addID(long val) throws IOException;

  public abstract long sizeInBytes();

  public abstract LongRandomAccessIterator iterator();

}
