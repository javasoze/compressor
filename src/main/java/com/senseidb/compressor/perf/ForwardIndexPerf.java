package com.senseidb.compressor.perf;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.lucene.util.packed.PackedInts.Reader;
import org.apache.lucene.util.packed.PackedInts.ReaderIterator;

import com.senseidb.compressor.idset.DirectForwardIndex;
import com.senseidb.compressor.idset.ForwardIndex;
import com.senseidb.compressor.idset.PackedForwardIndex;

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
    ForwardIndex idx = getPacked(numTerms, numElems);

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
    for (int i = 0; i < numLookups; ++i) {
      int val = rand.nextInt(numElems);
      tmp += idx.get(val);
    }
    end = System.currentTimeMillis();

    System.out.println("random access took: " + (end - start));
    
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    start = System.currentTimeMillis();
    idx.save(bout);
    bout.flush();
    byte[] bytes = bout.toByteArray();
    
    System.out.println("data size: "+bytes.length);

    ByteBuffer offheapMem = ByteBuffer.allocateDirect(bytes.length);
    offheapMem.put(bytes);
    offheapMem.rewind();
    end = System.currentTimeMillis();
    
    System.out.println("saving took: " + (end - start));
    /*
    Reader reader = PackedForwardIndex.load(offheapMem);

    start = System.currentTimeMillis();
    int s = reader.size();
    for (int i=0;i<s;++i){
      tmp += reader.get(i);
    }
    end = System.currentTimeMillis();

    System.out.println("random access 2 took: " + (end - start));
    */
    offheapMem.rewind();
    ReaderIterator iter = idx.iterator(offheapMem);
    
    start = System.currentTimeMillis();
    int count = iter.size();
    for (int i=0;i<count;++i){
      tmp += iter.next();
    }
    end = System.currentTimeMillis();
    

    System.out.println("iteration took: " + (end - start));
  }
}
