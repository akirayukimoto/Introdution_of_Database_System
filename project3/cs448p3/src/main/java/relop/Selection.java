package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
    private Iterator iterator = null;
    private Predicate[] predicates = null;
//    private boolean startSelect;
    private Tuple nextTuple;

    private boolean nextTupleIsConsumed;
	
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator aIter, Predicate... aPreds) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      this.iterator = aIter;
      this.predicates = aPreds;
      this.schema = iterator.getSchema();

      this.nextTupleIsConsumed = true;
//      startSelect = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  throw new UnsupportedOperationException("Not implemented");
    //Your code here

  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      iterator.restart();
//      startSelect = true;
      nextTupleIsConsumed = true;
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      return iterator.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      iterator.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      if (!nextTupleIsConsumed)
          return true;
//
      while (iterator.hasNext()) {
          nextTuple = iterator.getNext();
          for (int i = 0; i < predicates.length; i++) {
              if (predicates[i].evaluate(nextTuple)) {
                  nextTupleIsConsumed = false;
                  return true;
              }
          }
      }
      return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      nextTupleIsConsumed = true;
      return nextTuple;

  }

} // public class Selection extends Iterator
