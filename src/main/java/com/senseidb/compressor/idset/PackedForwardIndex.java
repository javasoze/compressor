package com.senseidb.compressor.idset;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

import com.senseidb.compressor.util.CompressorUtil;

public class PackedForwardIndex implements ForwardIndex {

  private Mutable data;
  public PackedForwardIndex(int numDocs, int numTerms){
    int bitsPerVal = CompressorUtil.getNumBits(numTerms);
    data = PackedInts.getMutable(numDocs, bitsPerVal, PackedInts.DEFAULT);
  }
  
  @Override
  public void add(int idx, long val) {
    data.set(idx, val);
  }

  @Override
  public long sizeInBytes() {
    return data.ramBytesUsed();
  }
  
  @Override
  public long get(int idx) {
    return data.get(idx);
  }

  @Override
  public void save(DataOutput out) throws IOException{
    //DataOutput dout = new DirectByteBufferDataOutput(out);
    data.save(out);
  }
  
  @Override
  public Reader load(DataInput input) throws IOException{
    //DataInput din = new DirectByteBufferDataInput(input);
    Reader reader = PackedInts.getReader(input);
    return reader;
  }

  @Override
  public ReaderIterator iterator(DataInput input) throws IOException{
    //DataInput din = new DirectByteBufferDataInput(input);
    return PackedInts.getReaderIterator(input, PackedInts.DEFAULT_BUFFER_SIZE);
  }
}
