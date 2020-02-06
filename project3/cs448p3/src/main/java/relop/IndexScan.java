package relop;

import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
    private HeapFile heapFile = null;
    //private HeapScan heapScan = null;
    private BucketScan bucketScan = null;
    private HashIndex hashIndex;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      this.schema = schema;
      this.heapFile = file;
      this.hashIndex = index;
      bucketScan = hashIndex.openScan();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      String str = hashIndex.toString();
      System.out.println("IndexScan: " + str);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      bucketScan.close();
      bucketScan = hashIndex.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return (hashIndex != null && bucketScan != null);
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      bucketScan.close();
//      heapFile.deleteFile();
//      heapFile = null;
//      hashIndex = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return bucketScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      if (bucketScan.hasNext()) {
          return new Tuple(this.schema, heapFile.selectRecord(bucketScan.getNext()));
      }
      else {
          throw new IllegalStateException("No Next Tuples");
      }
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return bucketScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return bucketScan.getNextHash();
  }

} // public class IndexScan extends Iterator
