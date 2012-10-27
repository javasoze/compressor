package com.senseidb.compressor.idset;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

import com.senseidb.compressor.io.ByteBufferInputStream;
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
  public void save(OutputStream out) throws IOException{
    DataOutput dout = new OutputStreamDataOutput(out);
    data.save(dout);
  }
  
  public static Reader load(ByteBuffer input) throws IOException{
    ByteBufferInputStream bbin = new ByteBufferInputStream(input);
    DataInput din = new InputStreamDataInput(bbin);
    Reader reader = PackedInts.getReader(din);
    return reader;
  }

  @Override
  public ReaderIterator iterator(ByteBuffer input) throws IOException{
    ByteBufferInputStream bbin = new ByteBufferInputStream(input);
    DataInput din = new InputStreamDataInput(bbin);
    return PackedInts.getReaderIterator(din, PackedInts.DEFAULT_BUFFER_SIZE);
  }
}
