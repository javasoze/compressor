package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collection;

public class ConjunctionDocIdSetIterator extends DocIdSetIterator {

  int lastReturn = -1;
  private DocIdSetIterator[] iterators = null;

  ConjunctionDocIdSetIterator(Collection<DocIdSetIterator> iteratorList) throws IOException{
    if (iteratorList == null || iteratorList.size() < 1)
      throw new IllegalArgumentException("Minimum one iterator required");
    iterators = iteratorList.toArray(new DocIdSetIterator[0]);
    lastReturn = (iterators.length > 0 ? -1 : DocIdSetIterator.NO_MORE_DOCS);
  }

  @Override
  public final int docID() {
    return lastReturn;
  }
  
  @Override
  public final int nextDoc() throws IOException {
    
    if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;
    
    DocIdSetIterator dcit = iterators[0];
    int target = dcit.nextDoc();
    int size = iterators.length;
    int skip = 0;
    int i = 1;
    while (i < size) {
      if (i != skip) {
        dcit = iterators[i];
        int docid = dcit.advance(target);
        
        if (docid > target) {
          target = docid;
          if(i != 0) {
            skip = i;
            i = 0;
            continue;
          }
          else
            skip = 0;
        }
      }
      i++;
    }
//    if(target != DocIdSetIterator.NO_MORE_DOCS)
//      _interSectionResult.add(target);
    return (lastReturn = target);
  }


  
  @Override
  public final int advance(int target) throws IOException {

    if (lastReturn == DocIdSetIterator.NO_MORE_DOCS) return DocIdSetIterator.NO_MORE_DOCS;
    
    DocIdSetIterator dcit = iterators[0];
    target = dcit.advance(target);
    int size = iterators.length;
    int skip = 0;
    int i = 1;
    while (i < size) {
      if (i != skip) {
        dcit = iterators[i];
        int docid = dcit.advance(target);
        if (docid > target) {
          target = docid;
          if(i != 0) {
            skip = i;
            i = 0;
            continue;
          }
          else
            skip = 0;
        }
      }
      i++;
    }
    return (lastReturn = target);
  }
  
  public static void main(String[] args) throws Exception{
    
  }
}
