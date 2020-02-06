package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

import java.util.ArrayList;

public class HashJoin extends Iterator {
	private IndexScan outerScan;
	private IndexScan innerScan;

	private Iterator outer;
	private Iterator inner;
	private int outerKey;
	private int innerKey;

	private boolean startJoin;
	private int currentBucket;

//	private Tuple nextTuples[];
	private ArrayList<Tuple> resultTuples;
	private HashTableDup hashJoinDup;

	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2){
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
//		this.outerScan = (IndexScan)aIter1;
//		this.innerScan = (IndexScan)aIter2;

		this.outer = aIter1;
		this.inner = aIter2;

		this.outerKey = aJoinCol1;
		this.innerKey = aJoinCol2;
//		this.nextTuples = null;
		this.resultTuples = new ArrayList<Tuple>();
		this.hashJoinDup = new HashTableDup();

		this.startJoin = true;
		this.currentBucket = 0;

		HeapFile outerHeapFile = new HeapFile(null);
		HeapFile innerHeapFile = new HeapFile(null);

		HashIndex outerHashIndex = new HashIndex(null);
		HashIndex innerHashIndex = new HashIndex(null);

		this.outerScan = build_scanner(outer, outerKey, outerHeapFile, outerHashIndex);
		this.innerScan = build_scanner(inner, innerKey, innerHeapFile, innerHashIndex);

		this.schema = schema.join(outerScan.schema, innerScan.schema);
	}

	private IndexScan build_scanner(Iterator it, int itKey, HeapFile heapFile, HashIndex hashIndex) {

		while (it.hasNext()) {
			Tuple curTuple = it.getNext();
			SearchKey searchKey = new SearchKey(curTuple.getField(itKey));
			hashIndex.insertEntry(searchKey, heapFile.insertRecord(curTuple.getData()));
		}

		return new IndexScan(it.getSchema(), hashIndex, heapFile);
	}

	@Override
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		outer.restart();
		inner.restart();
		startJoin = true;
	}

	@Override
	public boolean isOpen() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		return (outer.isOpen() && inner.isOpen());
	}

	@Override
	public void close() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		outer.close();
		inner.close();
		outerScan.close();
		innerScan.close();
	}



	@Override
	public boolean hasNext() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
//		while (true) {
//
//		}
		if (!resultTuples.isEmpty()) {
			return true;
		}

		Boolean result = false;
		while ((!result) && (outerScan.hasNext() || innerScan.hasNext())) {

			while (outerScan.hasNext() && currentBucket == outerScan.getNextHash()) {
				Tuple curTuple = outerScan.getNext();
				SearchKey curKey = new SearchKey(curTuple.getField(outerKey));
				hashJoinDup.add(curKey, curTuple);

			}

			while (innerScan.hasNext() && innerScan.getNextHash() < currentBucket && currentBucket != 0) {
				innerScan.getNext();
			}

			while (innerScan.hasNext() && innerScan.getNextHash() == currentBucket) {
				Tuple checker = innerScan.getNext();
				Tuple nextTuples[] = hashJoinDup.getAll(innerScan.getLastKey());

//				if (nextTuples != null) {
					for (Tuple tuple : nextTuples) {
//						if (checker.getField(innerKey).equals(nextTuples[i].getField(outerKey))) {
							Tuple curTuple = Tuple.join(tuple, checker, this.schema);
							resultTuples.add(curTuple);
							result = true;
							//return true;
//						}
					}
//					return true;
//				}
			}
			hashJoinDup.clear();

			currentBucket = outerScan.getNextHash();
		}
		return result;
//		nextTuples = null;
	}

	@Override
	public Tuple getNext() throws IllegalStateException{
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
//		if (resultTuples.isEmpty()) {
//			throw new IllegalStateException("No next tuples");
//		}
		Tuple resultTuple = resultTuples.get(0);
		resultTuples.remove(0);
		return resultTuple;
	}
} // end class HashJoin;
