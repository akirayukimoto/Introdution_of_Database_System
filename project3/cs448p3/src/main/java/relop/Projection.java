package relop;


/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	private Iterator iterator;
	private Integer[] fields;

//	private Tuple nextTuple;
//	private boolean nextTupleIsConsumed;
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator aIter, Integer... aFields) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      this.iterator = aIter;
      this.fields = aFields;

      this.schema = new Schema(fields.length);
      Schema orig = this.iterator.schema;
      for (int i = 0; i < fields.length; i++) {
          this.schema.initField(i, orig.fieldType(aFields[i]), orig.fieldLength(aFields[i]), orig.fieldName(aFields[i]));
      }

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
      return iterator.isOpen() && iterator.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      Tuple nextTuple = iterator.getNext();
      Tuple res = new Tuple(this.schema);
      for (int i = 0; i < fields.length; i++) {
          res.setField(i, nextTuple.getField(fields[i]));
      }
      return res;
  }

} // public class Projection extends Iterator
