package com.senseidb.compressor.perf;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts.Reader;

import com.senseidb.compressor.idset.DirectForwardIndex;
import com.senseidb.compressor.idset.ForwardIndex;
import com.senseidb.compressor.idset.IdSet.LongRandomAccessIterator;
import com.senseidb.compressor.idset.PackedForwardIndex;
import com.senseidb.compressor.util.CompressorUtil;

public class ForwardIndexPerf {

  
  static ForwardIndex getPacked(int numTerms, int numElems){
    return new PackedForwardIndex(numElems,numTerms);
  }
  
  static ForwardIndex getDirect(int numTerms, int numElems){
    return new DirectForwardIndex(numElems);
  }
  
  
  
  public static void main(String[] args) throws Exception{
    int numElems = 100000000;
    int numTerms = 100000;

    Random rand = new Random();
    ForwardIndex idx = getDirect(numTerms,numElems);//getPacked(numTerms, numElems);

    long start = System.currentTimeMillis();
    for (int i = 0; i < numElems; ++i) {
      int val = rand.nextInt(numTerms);
      idx.add(i, val);
    }
    long end = System.currentTimeMillis();

    System.out.println("loading took: " + (end - start));
    
    int numLookups = 1000000;
    start = System.currentTimeMillis();
    int tmp = 0;
    for (int i = 0; i < numElems; ++i) {
      int val = rand.nextInt(numElems);
      tmp += idx.get(val);
    }
    end = System.currentTimeMillis();

    System.out.println("random access took: " + (end - start));
    
    start = System.currentTimeMillis();
    
    for (int i = 0; i < numElems; ++i) {
      tmp += idx.get(i);
    }
    end = System.currentTimeMillis();

    System.out.println("mem iterate: " + (end - start));
    
    System.out.println("size: "+idx.sizeInBytes());
    ByteBuffer offheapMem = ByteBuffer.allocateDirect((int)idx.sizeInBytes()+9); // 9 byes of header
    //ByteBuffer offheapMem = ByteBuffer.allocate((int)idx.sizeInBytes()+9); // 9 byes of header
    DataOutput dout = CompressorUtil.getDataOutput(offheapMem);
    start = System.currentTimeMillis();
    idx.save(dout);
        
    offheapMem.rewind();
    end = System.currentTimeMillis();
    
    System.out.println("saving took: " + (end - start));
    
    Reader reader = idx.load(CompressorUtil.getDataInput(offheapMem));

    start = System.currentTimeMillis();
    int s = reader.size();
    for (int i=0;i<s;++i){
      tmp += reader.get(i);
    }
    end = System.currentTimeMillis();

    System.out.println("iterate in array took: " + (end - start));
    
    offheapMem.rewind();
    
    LongRandomAccessIterator iter = idx.iterator(CompressorUtil.getDataInput(offheapMem));
    
    start = System.currentTimeMillis();
    while(iter.hasNext()){
      tmp += iter.next();
    }
    end = System.currentTimeMillis();
    

    System.out.println("stream iterate took: " + (end - start));
    
System.out.println("iterate in array took: " + (end - start));
    
    offheapMem.rewind();
    iter = idx.iterator(CompressorUtil.getDataInput(offheapMem));
    
    start = System.currentTimeMillis();
    while(iter.hasNext()){
      tmp += iter.next();
    }
    end = System.currentTimeMillis();
    
    System.out.println("stream iterate again took: " + (end - start));
  }
}
