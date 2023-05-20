package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD{
	
	public String tableName;
	
	FlameRDDImpl(String T) {
		tableName = T;
	}

	public String getName() {
		return this.tableName;
	}
	
	// collect() should return a list that contains all the elements in the RDD.
	// all the elements are put into a unique row
	@Override
	public List<String> collect() throws Exception {
		List<String> RDDLst = new ArrayList<>();
		Iterator<Row> rowIter = Master.kvs.scan(tableName);
		while (rowIter.hasNext()) {
			Row currRow = rowIter.next();
			RDDLst.add(currRow.get("value"));
		}
		
		return RDDLst;
	}
	
	// flatMap() should invoke the provided lambda once for each element of the RDD, 
	// it should return a new RDD that contains all the strings from the Iterables the lambda invocations have returned.
	// It is okay for the same string to appear more than once in the output;
	// in this case, the RDD should contain multiple copies of that string.
	// The lambda is allowed to return null or an empty Iterable.
	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/flatMap", serializedLambda, this.tableName, null, 1, null);
		FlameRDDImpl rdd = new FlameRDDImpl(outputT);
		return rdd;
	}
	
	// mapToPair() should invoke the provided lambda once for each element
	// of the RDD, and should return a PairRDD that contains all the pairs
	// that the lambda returns. The lambda is allowed to return null, and
	// different invocations can return pairs with the same keys and/or the
	// same values.
	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/mapToPair", serializedLambda, this.tableName, null, 1, null);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}

	// returns an RDD that contains only the elements that are present in both RDDs. 
	// The result should contain only one instance of each element x, 
	// even if both input RDDs contain multiple instances of x.
	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		return null;
	}
	
	// sample() should return a new RDD that contains each element in the 
	// original RDD with the probability that is given as an argument.
	// If the original RDD contains multiple instances of the same element,
	// each instance should be sampled individually. This method is extra
	// credit on HW6 and should return 'null' if this EC is not implemented.
	@Override
	public FlameRDD sample(double f) throws Exception {
		String outputT = FlameContextImpl.invokeOperation("/rdd/sample", null, this.tableName, null, f, null);
		FlameRDD rdd = new FlameRDDImpl(outputT);
		return rdd;
	}
	
	// groupBy() should apply the given lambda to each element in the RDD
	// and return a PairRDD with elements (k, V), where k is a string that
	// the lambda returned for at least one input element and V is a
	// comma-separated list of elements in the original RDD for which the
	// lambda returned k. This method is extra credit on HW6 and should 
	// return 'null' if this EC is not implemented.
	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/groupBy", serializedLambda, this.tableName, null, 1, null);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		FlamePairRDDImpl res = pairRDD.fold((a,b) -> (a.equals(""))? b:a+","+b);
		return res;
	}
	
	// return the size of the table in the KVS
	// count() should return the number of elements in this RDD. 
	// Duplicate elements should be included in the count.
	@Override
	public int count() throws Exception {
		int cnt = Master.kvs.count(tableName);
		return cnt;
	}
	
	// saveAsTable() should cause a table with the specified name to appear 
	// in the KVS that contains the data from this RDD. The table should 
	// have a row for each element of the RDD, and the element should be
	// in a column called 'value'; the key can be anything. 
	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		Master.kvs.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	// distinct() should return a new RDD that contains the same
	// elements, except that, if the current RDD contains multiple
	// copies of some elements, the new RDD should contain only one copy of those elements.
	@Override
	public FlameRDD distinct() throws Exception {
		String outputT = FlameContextImpl.invokeOperation("/rdd/distinct", null, this.tableName, null, 1.0, null);
		FlameRDD rdd = new FlameRDDImpl(outputT);
		return rdd;
	}
	
	// scan the underlying table using KVSClient and return the values in the first n rows (or fewer if the RDD isn’t large enough)
	// take() should return up 'num' elements from the RDD, or all
	// elements, if the RDD contains fewer than 'num' elements.
	// If the RDD contains more than 'num' elements, any subset
	// of size 'num' may be returned.
	@Override
	public Vector<String> take(int num) throws Exception {
		Vector<String> firstNumRow = new Vector<String>();
		Iterator<Row> rowIter = Master.kvs.scan(tableName);
		int counter = 0;
		while (rowIter.hasNext() && counter < num) {
			Row currRow = rowIter.next();
			firstNumRow.add(currRow.get("value"));
			counter += 1;
		}
		return firstNumRow;
	}
	
	// fold() should call the provided lambda for each element of the RDD, 
	// with that element as the second argument. 
	// In the first invocation, the first argument should be 'zeroElement'; 
	// in each subsequent invocation, the first argument should be the result of the previous invocation. 
	// The function returns the result of the last invocation, 
	// or 'zeroElement' if the RDD does not contain any elements.
	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/fold", serializedLambda, this.tableName, zeroElement, 1, null);
		Iterator<Row> rowIter = Master.kvs.scan(outputT);
		String accumulator = zeroElement;
    	
        while (rowIter.hasNext()) {
			Row currRow = rowIter.next();
			accumulator = lambda.op(accumulator, currRow.get("value"));
        }
		
		return accumulator;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/flatMapToPair", serializedLambda, this.tableName, null, 1, null);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/filter", serializedLambda, this.tableName, null, 1, null);
		FlameRDDImpl rdd = new FlameRDDImpl(outputT);
		return rdd;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/rdd/mapPartitions", serializedLambda, this.tableName, null, 1, null);
		FlameRDDImpl rdd = new FlameRDDImpl(outputT);
		return rdd;
	}

}
