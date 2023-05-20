package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.*;

class Worker extends cis5550.generic.Worker {
	private static final Logger logger = Logger.getLogger(Worker.class);

	public static void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Syntax: Worker <port> <masterIP:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		String server = args[1];
		startPingThread(server, "" + port, port + "");
		final File myJAR = new File("__worker" + port + "-current.jar");

		port(port);

		post("/useJAR", (request, response) -> {
			FileOutputStream fos = new FileOutputStream(myJAR);
			fos.write(request.bodyAsBytes());
			fos.close();
			return "OK";
		});

		post("/rdd/flatMap", (request, response) -> {
			// decode the query parameters
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			// deserialize the lambda using Serializer;
			// be sure to provide the name of the job’s JAR file
			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			StringToIterable func = (StringToIterable) lambda;

			// use KVSClient to scan the keys in this range,
			// use the op method to invoke the lambda on the value column in each row
			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			while (iter.hasNext()) {
				Row currRow = iter.next();
				Iterable<String> i = func.op(currRow.get("value"));
				// if not null, iterate over the elements and put them into the output table in
				// the KVS
				// be sure to make the row keys unique
				// so, if the lambda returns the same value more than once, the values will all
				// show up in separate rows instead of overwriting each other.
				ByteArrayOutputStream out = new ByteArrayOutputStream();

				if (i != null) {
					Iterator<String> iIter = i.iterator();
					while (iIter.hasNext()) {
						String val = iIter.next();
						String R = Hasher.hash(UUID.randomUUID().toString());
						Row curRow = new Row(R);
						curRow.put("value", val);

						out.write(curRow.toByteArray());
						out.write("\n".getBytes());
//					kvs.put(outputT, R, "value", val);
					}
					out.close();
					if (out.size() > 0) {kvs.putRows(outputT,out.toByteArray());}
				}
			}
			return "OK";
		});

		post("/rdd/mapToPair", (request, response) -> {
			// decode the query parameters
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			StringToPair func = (StringToPair) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
			// hand the string from the input table to the lambda and get back a pair (k,v);
			// v will need to go into a row k in the output table, with a unique column
			// name.
			// Since the input table already has a unique key for each element,
			// you can simply use that key (which is otherwise meaningless) as the column
			// name.
			while (iter.hasNext()) {
				Row currRow = iter.next();
				FlamePair i = func.op(currRow.get("value"));
				if (i != null) {
					String k = i._1();
					String val = i._2();
//				try {
//					kvs.put(outputT, k, currRow.key(), val);
//				} catch (Exception e) {
//					logger.error(e.getMessage());
//				}
					Row r = new Row(k);
					r.put(currRow.key(), val);

					out.write(r.toByteArray());
					out.write("\n".getBytes());
					cnt++;
					if (cnt == 500) {
						cnt = 0;
						out.close();
						kvs.putRows2(outputT, out.toByteArray());
						out = new ByteArrayOutputStream();
					}
				}

			}
			out.close();
			kvs.putRows2(outputT, out.toByteArray());
			return "OK";
		});

		post("/pairRDD/foldByKey", (request, response) -> {
			// decode the query parameters
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");
			String zeroElement = request.queryParams("zeroElement");
			zeroElement = URLEncoder.encode(zeroElement, "UTF-8");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			TwoStringsToString func = (TwoStringsToString) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
//    	For each Row the worker finds, it should initialize an “accumulator” with the zero element, 
//    	then iterate over all the columns and call the lambda for each value 
//    	(with the value and the current accumulator as arguments) to compute a new accumulator. 
//    	Finally, it should insert a key-value pair into the output table, 
//    	whose key is identical to the key in the input table, 
//    	and whose value is the final value of the accumulator. 
//    	In this case, the column name can be anything because there will be only one value for each key in the output table.
//    	MY NOTE: original table is regrouped by mapToPair (split into colVal[0] and colVal[1], and make former the column name
			while (iter.hasNext()) {
				Row currRow = iter.next();
				String accumulator = zeroElement;
				for (String C : currRow.columns()) {
					accumulator = func.op(accumulator, currRow.get(C));
				}
//				kvs.put(outputT, currRow.key(), "randomC", accumulator);
				Row r = new Row(currRow.key());
				r.put("randomC", accumulator);

				out.write(r.toByteArray());
				out.write("\n".getBytes());
				cnt++;
				if (cnt == 500) {
					cnt = 0;
					out.close();
					kvs.putRows(outputT, out.toByteArray());
					out = new ByteArrayOutputStream();
				}
			}
			out.close();
			kvs.putRows(outputT, out.toByteArray());
			return "OK";
		});

		post("/rdd/sample", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");
			String sample = request.queryParams("sample");
			double sampleRatio = Double.parseDouble(sample);

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			while (iter.hasNext()) {
				Row currRow = iter.next();
				String val = currRow.get("value");
				Random rand = new Random();
				int num = rand.nextDouble() <= sampleRatio ? 1 : 0;
				if (num == 1) {
					kvs.put(outputT, currRow.key(), "value", val);
				}
			}
			return "OK";
		});

		post("/rdd/groupBy", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			StringToString func = (StringToString) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			while (iter.hasNext()) {
				Row currRow = iter.next();
				String currVal = currRow.get("value");
				String k = func.op(currVal);
				String C = Hasher.hash(UUID.randomUUID().toString());
				kvs.put(outputT, k, C, currVal);
			}
			return "OK";
		});

		post("/pairRDD/fold", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			TwoStringsToString func = (TwoStringsToString) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
			while (iter.hasNext()) {
				Row currRow = iter.next();
				String accumulator = "";
				for (String C : currRow.columns()) {
					accumulator = func.op(accumulator, currRow.get(C));
				}
//				kvs.put(outputT, currRow.key(), "value", accumulator);
				Row r = new Row(currRow.key());
				r.put("value", accumulator);

				out.write(r.toByteArray());
				out.write("\n".getBytes());
				cnt++;
				if (cnt == 500) {
					cnt = 0;
					out.close();
					kvs.putRows(outputT, out.toByteArray());
					out = new ByteArrayOutputStream();
				}
			}
			out.close();
			kvs.putRows(outputT, out.toByteArray());
			return "OK";
		});

		post("/cxt/fromTable", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			RowToString func = (RowToString) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			// scan the range of the input table that the worker has been assigned,
			// invoke the lambda for each row it finds,
			// and store the resulting string (if any) in the output table.
			// lambda is allowed to return null for certain Rows;
			// when it does, no data should be added to the RDD for these Rows.
			while (iter.hasNext()) {
				Row currRow = iter.next();
				String val = func.op(currRow);
				if (val != null) {
					String R = Hasher.hash(currRow.key());
					kvs.put(outputT, R, "value", val);
				}
			}
			return "OK";
		});

		// flatMap() should invoke the provided lambda once for each pair in the
		// PairRDD,
		// and it should return a new RDD that contains all the strings from the
		// Iterables
		// the lambda invocations have returned. It is okay for the same string to
		// appear
		// more than once in the output; in this case, the RDD should contain multiple
		// copies of that string. The lambda is allowed to return null or an empty
		// Iterable.
		post("/pairRDD/flatMap", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			PairToStringIterable func = (PairToStringIterable) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			while (iter.hasNext()) {
				Row currRow = iter.next();
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				for (String C : currRow.columns()) {
					FlamePair pair = new FlamePair(currRow.key(), currRow.get(C));
					Iterable<String> i = func.op(pair);
					if (i != null) {
						Iterator<String> iIter = i.iterator();
						while (iIter.hasNext()) {
							String val = iIter.next();
							String R = Hasher.hash(UUID.randomUUID().toString());
//							kvs.put(outputT, R, "value", val);
							Row r = new Row(R);
							r.put("value", val);

							out.write(r.toByteArray());
							out.write("\n".getBytes());
						}
					}
				}
				out.close();
				kvs.putRows(outputT, out.toByteArray());
			}
			return "OK";
		});

		post("/pairRDD/flatMapToPair", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			PairToPairIterable func = (PairToPairIterable) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
			while (iter.hasNext()) {
				Row currRow = iter.next();
				for (String C : currRow.columns()) {
					// iterate through each pair
					FlamePair pair = new FlamePair(currRow.key(), currRow.get(C));
					Iterable<FlamePair> i = func.op(pair);
					// put each pair after lambda into a pairRDD
					if (i != null) {
						Iterator<FlamePair> iIter = i.iterator();
						while (iIter.hasNext()) {
							FlamePair iPair = iIter.next();
							String k = iPair._1();
							String val = iPair._2();
							String newC = Hasher.hash(UUID.randomUUID().toString());
//							try {
//								kvs.put(outputT, k, newC, val);
//							} catch (Exception e) {
//								logger.error(e.getMessage());
//							}
							Row r = new Row(k);
							r.put(newC, val);

							out.write(r.toByteArray());
							out.write("\n".getBytes());
							cnt++;
							if (cnt == 500) {
								out.close();
								kvs.putRows2(outputT, out.toByteArray());
								out = new ByteArrayOutputStream();
								cnt = 0;
							}
						}
					}
				}
			}
			out.close();
			kvs.putRows2(outputT, out.toByteArray());
			return "OK";
		});

		post("/rdd/flatMapToPair", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			StringToPairIterable func = (StringToPairIterable) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
			while (iter.hasNext()) {
				Row currRow = iter.next();
				Iterable<FlamePair> i = func.op(currRow.get("value"));
				// put each pair after lambda into a pairRDD
				if (i != null) {
					Iterator<FlamePair> iIter = i.iterator();
					while (iIter.hasNext()) {
						FlamePair iPair = iIter.next();
						String k = iPair._1();
						String val = iPair._2();
						String newC = Hasher.hash(UUID.randomUUID().toString());
//						kvs.put(outputT, k, newC, val);
						Row r = new Row(k);
						r.put(newC, val);

						out.write(r.toByteArray());
						out.write("\n".getBytes());
						cnt++;
						if (cnt == 500) {
							out.close();
							kvs.putRows2(outputT, out.toByteArray());
							out = new ByteArrayOutputStream();
							cnt = 0;
						}
					}
				}
			}
			out.close();
			kvs.putRows2(outputT, out.toByteArray());
			return "OK";
		});

		// put each value v from the input table into a row with key v (and column name
		// value, as usual)!
		// If the RDD contains duplicates, they will overwrite each other,
		// and leave only a single instance at the end.
		post("/rdd/distinct", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			while (iter.hasNext()) {
				Row currRow = iter.next();
				String colVal = currRow.get("value");
				kvs.put(outputT, colVal, "value", colVal);
			}
			return "OK";
		});

		// Suppose A contains a pair (k,v_A) and B contains a pair (k,v_B).
		// Then the result should contain a pair (k,v_A+","+v_B).
		post("/pairRDD/join", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");
			String otherT = request.queryParams("otherT");

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
			while (iter.hasNext()) {
				Row currRow = iter.next();
				Row otherRow = kvs.getRow(otherT, currRow.key());
				if (otherRow != null) {
					for (String C1 : currRow.columns()) {
						for (String C2 : otherRow.columns()) {
							String colVal1 = currRow.get(C1);
							String colVal2 = otherRow.get(C2);
							if (colVal1 != null & colVal2 != null) {
								String C = Hasher.hash(C1 + "," + C2);
//								kvs.put(outputT, currRow.key(), C, colVal1 + "," + colVal2);
								Row r = new Row(currRow.key());
								r.put(C, colVal1 + "," + colVal2);

								out.write(r.toByteArray());
								out.write("\n".getBytes());
								cnt++;
								if (cnt == 500) {
									out.close();
									kvs.putRows2(outputT, out.toByteArray());
									out = new ByteArrayOutputStream();
									cnt = 0;
								}
							}
						}
					}
				}

			}
			out.close();
			kvs.putRows2(outputT, out.toByteArray());
			return "OK";
		});

		// aggregate over an entire range of KVS keys on each worker, not just over the
		// values in a given Row.
		post("/rdd/fold", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");
			String zeroElement = request.queryParams("zeroElement");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			TwoStringsToString func = (TwoStringsToString) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);
			String accumulator = zeroElement;

			while (iter.hasNext()) {
				Row currRow = iter.next();
				accumulator = func.op(accumulator, currRow.get("value"));
			}
			String R = Hasher.hash(UUID.randomUUID().toString());
			kvs.put(outputT, R, "value", accumulator);
			return "OK";
		});

		post("/rdd/filter", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			StringToBoolean func = (StringToBoolean) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			while (iter.hasNext()) {
				Row currRow = iter.next();
				boolean predicate = func.op(currRow.get("value"));
				if (predicate) {
					kvs.put(outputT, currRow.key(), "value", currRow.get("value"));
				}
			}
			return "OK";
		});

		// The lambda should be invoked once on each worker,
		// with an iterator that contains the RDD elements that worker is working on
		// the elements in the iterator that L returns should be stored in another RDD,
		// which mapPartitions should return.
		post("/rdd/mapPartitions", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");

			Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			IteratorToIterator func = (IteratorToIterator) lambda;

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> iter = kvs.scan(inputT, fromKey, toKeyExclusive);

			List<String> valLst = new ArrayList<String>();

			while (iter.hasNext()) {
				Row currRow = iter.next();
				valLst.add(currRow.get("value"));
			}
			Iterator<String> valIter = valLst.iterator();
			Iterator<String> resIter = func.op(valIter);
			if (resIter != null) {
				while (resIter.hasNext()) {
					String val = resIter.next();
					String R = Hasher.hash(UUID.randomUUID().toString());
					kvs.put(outputT, R, "value", val);
				}
			}
			return "OK";
		});

		post("/pairRDD/cogroup", (request, response) -> {
			String inputT = request.queryParams("inputT");
			String outputT = request.queryParams("outputT");
			String hostAndPort = request.queryParams("hostAndPort");
			String fromKey = request.queryParams("fromKey");
			String toKeyExclusive = request.queryParams("toKeyExclusive");
			String otherT = request.queryParams("otherT");

			if (fromKey.equals("null")) {
				fromKey = null;
			}
			if (toKeyExclusive.equals("null")) {
				toKeyExclusive = null;
			}
			KVSClient kvs = new KVSClient(hostAndPort);
			Iterator<Row> currIter = kvs.scan(inputT, fromKey, toKeyExclusive);
			Iterator<Row> otherIter = kvs.scan(otherT, fromKey, toKeyExclusive);
			HashMap<String, HashMap<String, ArrayList<String>>> myMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
			// hash map: {rowkey1: {rdd1: [1,2,3], rdd2: [2,3]}, rowKey2: rdd2: [1]}

			while (currIter.hasNext()) {
				Row currRow = currIter.next();
				if (!myMap.containsKey(currRow.key())) {
					HashMap<String, ArrayList<String>> tempMap = new HashMap<String, ArrayList<String>>();
					tempMap.put("rdd1", new ArrayList<String>());
					myMap.put(currRow.key(), tempMap);
				}
				for (String C : currRow.columns()) {
					myMap.get(currRow.key()).get("rdd1").add(currRow.get(C));
				}
			}

			while (otherIter.hasNext()) {
				Row currRow = otherIter.next();
				if (!myMap.containsKey(currRow.key())) {
					HashMap<String, ArrayList<String>> tempMap = new HashMap<String, ArrayList<String>>();
					tempMap.put("rdd2", new ArrayList<String>());
					myMap.put(currRow.key(), tempMap);
				} else {
					if (!myMap.get(currRow.key()).containsKey("rdd2")) {
						myMap.get(currRow.key()).put("rdd2", new ArrayList<String>());
					}
				}
				for (String C : currRow.columns()) {
					myMap.get(currRow.key()).get("rdd2").add(currRow.get(C));
				}
			}

			for (String rowKey : myMap.keySet()) {
				String val1 = "";
				if (myMap.get(rowKey).containsKey("rdd1")) {
					for (String val : myMap.get(rowKey).get("rdd1")) {
						val1 = (val1 == "") ? val : val1 + "," + val;
					}
				}
				val1 = "[" + val1 + "]";
				String val2 = "";
				if (myMap.get(rowKey).containsKey("rdd2")) {
					for (String val : myMap.get(rowKey).get("rdd2")) {
						val2 = (val2 == "") ? val : val2 + "," + val;
					}
				}
				val2 = "[" + val2 + "]";
				kvs.put(outputT, rowKey, "value", val1 + "," + val2);
			}
			return "OK";
		});

	}
}
