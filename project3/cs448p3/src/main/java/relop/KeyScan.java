package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
    private HashIndex hashIndex;
    private HashScan hashScan;
    private HeapFile heapFile;
    private SearchKey searchKey;
    boolean isOpen;
	
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      this.schema = aSchema;
      this.hashIndex = aIndex;
      this.searchKey = aKey;
      this.heapFile = aFile;
      this.hashScan = hashIndex.openScan(searchKey);

      this.isOpen = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {

      //throw new UnsupportedOperationException("Not implemented");
      System.out.println("KeyScan: " + hashIndex.toString());
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");

    //Your code here
      hashScan.close();
      hashScan = hashIndex.openScan(searchKey);
      isOpen = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      return isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
      //throw new UnsupportedOperationException("Not implemented");
      //Your code here

      hashScan.close();
      isOpen = false;
//      searchKey = null;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      return isOpen && hashScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      return new Tuple(this.schema, heapFile.selectRecord(hashScan.getNext()));
  }

} // public class KeyScan extends Iterator
