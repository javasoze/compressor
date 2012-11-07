package com.senseidb.compressor.idset;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Mutable;
import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.Writer;

import com.senseidb.compressor.idset.IdSet.LongRandomAccessIterator;
import com.senseidb.compressor.util.CompressorUtil;

public class PackedForwardIndex implements ForwardIndex {

  private Mutable data;
  public PackedForwardIndex(int numDocs, int numTerms){
    int bitsPerVal = CompressorUtil.getNumBits(numTerms);
    data = PackedInts.getMutable(numDocs, bitsPerVal,PackedInts.DEFAULT);
  }
  
  @Override
  public void add(int idx, long val) {
    data.set(idx, val);
  }

  @Override
  public long sizeInBytes() {
    return data.getBitsPerValue()*data.size();
  }
  
  @Override
  public long get(int idx) {
    return data.get(idx);
  }

  @Override
  public void save(DataOutput out) throws IOException{
    int size = data.size();
    Writer writer = PackedInts.getWriter(out, data.size(), data.getBitsPerValue(),PackedInts.DEFAULT);
    for (int i=0;i<size;++i){
      long val = data.get(i);
      writer.add(val);
    }
    writer.finish();
    
  }
  
  @Override
  public Reader load(DataInput input) throws IOException{
    //DataInput din = new DirectByteBufferDataInput(input);
    Reader reader = PackedInts.getReader(input);
    return reader;
  }

  @Override
  public LongRandomAccessIterator iterator(DataInput input) throws IOException{
    final Reader reader = load(input);
    return new LongRandomAccessIterator(){

      int current = 0;
      @Override
      public boolean hasNext() throws IOException {
        return current < reader.size();
      }

      @Override
      public long next() throws IOException {
        return reader.get(current++);
      }

      @Override
      public void reset() {
        current = 0;
      }

      @Override
      public long get(int idx) throws IOException {
        return reader.get(idx);
      }

      @Override
      public long numElems() {
        return reader.size();
      }
      
    };
  }
}
