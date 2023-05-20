package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD{
	public String tableName;
	
	FlamePairRDDImpl(String T) {
		tableName = T;
	}

	public String getName() {
		return this.tableName;
	}
	
	// collect() should return a list that contains all the elements in the PairRDD.
	//	a single Row of data can contain several key-value pairs– 
	// so you will need to iterate over the columns() of each Row and, 
	// for each value v you find in a column, output a pair (k,v), 
	// where k is the key of the Row.
	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> PairRDDLst = new ArrayList<>();
		Iterator<Row> rowIter = Master.kvs.scan(tableName);
		while (rowIter.hasNext()) {
			Row currRow = rowIter.next();
			for (String C: currRow.columns()) {
				FlamePair pair = new FlamePair(currRow.key(), currRow.get(C));
				PairRDDLst.add(pair);
			}
		}
		
		return PairRDDLst;
	}
	
	
	// foldByKey() folds all the values that are associated with a given key in the
	// current PairRDD, and returns a new PairRDD with the resulting keys and values.
	// Formally, the new PairRDD should contain a pair (k,v) for each distinct key k 
	// in the current PairRDD, where v is computed as follows: Let v_1,...,v_N be the 
	// values associated with k in the current PairRDD (in other words, the current 
	// PairRDD contains (k,v_1),(k,v_2),...,(k,v_N)). Then the provided lambda should 
	// be invoked once for each v_i, with that v_i as the second argument. The first
	// invocation should use 'zeroElement' as its first argument, and each subsequent
	// invocation should use the result of the previous one. v is the result of the
	// last invocation.
	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/pairRDD/foldByKey", serializedLambda, this.tableName, zeroElement, 1, null);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}
	
	public FlamePairRDDImpl fold(TwoStringsToString lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/pairRDD/fold", serializedLambda, this.tableName, null, 1, null);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}

	@Override
	public int count() throws Exception {
		int cnt = Master.kvs.count(tableName);
		return cnt;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		Master.kvs.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
		
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/pairRDD/flatMap", serializedLambda, this.tableName, null, 1, null);
		FlameRDDImpl rdd = new FlameRDDImpl(outputT);
		return rdd;
	}
	
	// flatMapToPair() is analogous to flatMap(), except that the lambda returns pairs 
	// instead of strings, and that the output is a PairRDD instead of a normal RDD.
	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = FlameContextImpl.invokeOperation("/pairRDD/flatMapToPair", serializedLambda, this.tableName, null, 1, null);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}
	
	// join() joins the current PairRDD A with another PairRDD B. Suppose A contains
	// a pair (k,v_A) and B contains a pair (k,v_B). Then the result should contain
	// a pair (k,v_A+","+v_B).
	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		FlamePairRDDImpl otherRDD = (FlamePairRDDImpl)other;
		String otherT = otherRDD.tableName;
		String outputT = FlameContextImpl.invokeOperation("/pairRDD/join", null, this.tableName, null, 1, otherT);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}
	
	// extra credit
	// This method should return a new PairRDD that contains, for each key k that exists 
	// in either the original RDD or in R, a pair (k,"[X],[Y]"), where X and Y are 
	// comma-separated lists of the values from the original RDD and from R, respectively. 
	// For instance, if the original RDD contains (fruit,apple) and (fruit,banana) and 
	// R contains (fruit,cherry), (fruit,date) and (fruit,fig), the result should contain 
	// a pair with key fruit and value [apple,banana],[cherry,date,fig]. This method is 
	// extra credit in HW7; if you do not implement it, please return 'null'.
	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		FlamePairRDDImpl otherRDD = (FlamePairRDDImpl)other;
		String otherT = otherRDD.tableName;
		String outputT = FlameContextImpl.invokeOperation("/pairRDD/cogroup", null, this.tableName, null, 1, otherT);
		FlamePairRDDImpl pairRDD = new FlamePairRDDImpl(outputT);
		return pairRDD;
	}
	

}
