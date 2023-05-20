package cis5550.flame;

import static cis5550.flame.Master.kvs;

import java.util.List;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.Partitioner.Partition;
import java.util.UUID;
import java.util.Vector;

public class FlameContextImpl implements FlameContext {
	private static final Logger logger = Logger.getLogger(FlameContextImpl.class);
	public String jarName;
	public String out = "";
	public static int seqNum = 0;
	public static int partitionKeyRangePerWorker = 1;

	public FlameContextImpl(String jarName) {
		this.jarName = jarName;
	}

	@Override
	public KVSClient getKVS() {
		return Master.kvs;
	}

	// outputs are returned only when the job terminates
	// When a job invokes output(), your solution should store the provided string
	// and return it in the body of the /submit response, if and when the job
	// terminates normally.
	// If a job invokes output() more than once, the strings should be concatenated.
	// If a job never invokes output(), the body of the submit response should
	// contain a message
	// saying that there was no output.
	@Override
	public void output(String s) {
		out += s;
	}

	// This function should return a FlameRDD that contains the strings in the
	// provided List.
	// It is okay for this method to run directly on the master; it does not need to
	// be parallelized.
	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		String T = String.valueOf(System.currentTimeMillis()) + "-" + seqNum;
		seqNum += 1;
		for (int i = 0; i < list.size(); i++) {
			String R = Hasher.hash(UUID.randomUUID().toString());
			Master.kvs.put(T, R, "value", list.get(i));
		}
		FlameRDDImpl rdd = new FlameRDDImpl(T);

		return rdd;
	}

//	@Override
	public static String invokeOperation(String operation, byte[] lambda, String T, String zeroElement,
			double sampleRatio, String otherT) throws Exception {
		// generate a fresh name for the output table
		String outputT = String.valueOf(System.currentTimeMillis()) + "-" + seqNum;
//		Master.kvs.persist(outputT);

		seqNum += 1;

		// use Partitioner class to find good assignment of key ranges to Flame workers:
		// invoke addKVSWorker for each KVS worker and give it the worker’s address,
		// the worker’s ID as the start of the KVS key range for which this worker is
		// responsible,
		// and the next worker’s ID as the end of that range (exception: last worker)
		Partitioner partition = new Partitioner();
		partition.setKeyRangesPerWorker(partitionKeyRangePerWorker);

		if (Master.kvs.numWorkers() == 0) {
			System.out.println("No KVS workers!!");
			return null;
		}

		partition.addKVSWorker(Master.kvs.getWorkerAddress(Master.kvs.numWorkers() - 1), null,
				Master.kvs.getWorkerID(0));
		for (int i = 0; i < Master.kvs.numWorkers() - 1; i++) {
			partition.addKVSWorker(Master.kvs.getWorkerAddress(i), Master.kvs.getWorkerID(i),
					Master.kvs.getWorkerID(i + 1));
		}
		partition.addKVSWorker(Master.kvs.getWorkerAddress(Master.kvs.numWorkers() - 1),
				Master.kvs.getWorkerID(Master.kvs.numWorkers() - 1), null);

		// call addFlameWorker for each Flame worker
		for (int i = 0; i < Master.getWorkers().size(); i++) {
			partition.addFlameWorker(Master.getWorkers().get(i));
		}

		// call partitioner's assignPartitions() -> return a vector of Partition objects
		// (key range + Flame worker)
		Vector<Partition> partitionLst = partition.assignPartitions();
		partitionLst.forEach(p -> {
			logger.info(p.toString());
		});

		// send a HTTP request to each worker to tell it what operation to perform
		// use HTTP.doRequest to send message; send all request in parallel through
		// multiple threads
		Thread threads[] = new Thread[partitionLst.size()];
		String results[] = new String[partitionLst.size()];

		for (int i = 0; i < partitionLst.size(); i++) {
			String url = "http://" + partitionLst.elementAt(i).assignedFlameWorker + operation;
			String qParams = "?inputT=" + T + "&outputT=" + outputT + "&hostAndPort=" + Master.kvs.getMaster() // kvs
																												// master
																												// port
																												// (localhost:8000)
					+ "&toKeyExclusive=" + partitionLst.elementAt(i).toKeyExclusive + "&fromKey="
					+ partitionLst.elementAt(i).fromKey + "&zeroElement=" + zeroElement + "&sample=" + sampleRatio
					+ "&otherT=" + otherT;
			final int j = i;
			threads[i] = new Thread("Invoke operation #" + (i + 1)) {
				public void run() {
					try {
						results[j] = new String(HTTP.doRequest("POST", url + qParams, lambda).body());
					} catch (Exception e) {
						results[j] = "Exception: " + e;
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}

		// wait for all responses to arrive, send back status code/failure msgs
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
				System.out.println("Fail to get all responses from threads!!");
				return null;
			}
		}

		for (int i = 0; i < results.length; i++) {
			if (!results[i].contains("OK")) {
				System.out.println("Get error on worker " + i + ": " + results[i]);
				return null;
			}
		}

		return outputT;
	}

	// This function should scan the table in the key-value store with the specified
	// name,
	// invoke the provided lambda with each Row of data from the KVS, and then
	// return
	// an RDD with all the strings that the lambda invocations returned. The lambda
	// is allowed to return null for certain Rows; when it does, no data should be
	// added to the RDD for these Rows. This method should run in parallel on all
	// the
	// workers, just like the RDD/PairRDD operations.
	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		byte[] serializedLambda = Serializer.objectToByteArray(lambda);
		String outputT = invokeOperation("/cxt/fromTable", serializedLambda, tableName, null, 1, null);
		FlameRDD rdd = new FlameRDDImpl(outputT);
		return rdd;
	}

	// This function should control how many separate key ranges each worker should
	// be assigned. If this function is never called, each worker should just get
	// a single key range. But if setConcurrencyLevel(k) is called, each worker
	// should get k separate, non-overlapping key ranges to work on, and the worker
	// should work on these ranges in parallel, e.g., on separate cores.
	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		this.partitionKeyRangePerWorker = keyRangesPerWorker;
	}

}
