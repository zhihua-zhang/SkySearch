package cis5550.jobs;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.flame.*;
import cis5550.tools.Stemmer;
import cis5550.tools.*;
import cis5550.kvs.*;

public class TFIDF {
	private static final Logger logger = Logger.getLogger(TFIDF.class);
	private static final String separator = ",";
    private static final String maxCountFlag = "##max##";
    private static final double tf_coef = 0.4;

	public static void run(FlameContext ctx, String[] urlArg) throws Exception {
		try {
			FlameRDD rdd = ctx.fromTable("inverseIndex", row -> {
                String ret = "";
                for (String col : row.columns()) {
                    ret = row.key() + separator + row.get(col);
                }
                return ret;
            });
			FlamePairRDD pairRDD = rdd.mapToPair(s -> {
				String[] str = s.split(separator, 2);
				return new FlamePair(str[0], str[1]);
			});
			ctx.getKVS().delete(rdd.getName());
            int numDoc = ctx.getKVS().count("forwardIndex");
			FlamePairRDD idfExec = pairRDD.flatMapToPair(pair -> {
				String word = pair._1();
                String[] urls = pair._2().split(separator);
				List<FlamePair> wuPairLst = new ArrayList<FlamePair>();
                int docCount = urls.length;
                for (String url : urls) {
                    double idf = Math.log((double)numDoc / docCount);
                    wuPairLst.add(new FlamePair(word, url + separator + idf));
                }
				Iterable<FlamePair> iter = wuPairLst;
				return iter;
			});
			ctx.getKVS().delete(pairRDD.getName());
			ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> TFIDF = updateIDF(ctx.getKVS(), idfExec);
			ctx.getKVS().delete(idfExec.getName());
			System.out.println("TFIDF: TFIDF(idf) size: " + TFIDF.size());
			logger.info("TFIDF: TFIDF(idf) size: " + TFIDF.size());

            rdd = ctx.fromTable("forwardIndex", row -> {
                String url = row.key();
                String infoStr = "";
                String maxCount = "";
                for (String word : row.columns()) {
                    if (word.equals(maxCountFlag)) {
                        maxCount = row.get(word);
                    } else {
                        String[] info = row.get(word).split(separator, 2);
                        infoStr += (separator + word + separator + info[0]);
                    }
                }
                return url + separator + maxCount + infoStr;
            });
			pairRDD = rdd.mapToPair(s -> {
				String[] str = s.split(separator, 2);
				return new FlamePair(str[0], str[1]);
			});
			ctx.getKVS().delete(rdd.getName());
			FlamePairRDD tfExec = pairRDD.flatMapToPair(pair -> {
				String url = pair._1();
				String[] str = pair._2().split(separator);
                int maxCount = Integer.valueOf(str[0]);
                List<FlamePair> pairLst = new ArrayList<FlamePair>();
                for(int i=1; i<str.length; i+=2) {
                    String word = str[i];
                    int count = Integer.valueOf(str[i+1]);
                    double tf = tf_coef + (1-tf_coef) * (double)count / maxCount;
                    pairLst.add(new FlamePair(url, word + separator + tf));
                }
				Iterable<FlamePair> iter = pairLst;
				return iter;
			});
			ctx.getKVS().delete(pairRDD.getName());
			updateTF(ctx.getKVS(), TFIDF, tfExec);
			ctx.getKVS().delete(tfExec.getName());
			System.out.println("TFIDF: TFIDF(tf) size: " + TFIDF.size());
			logger.info("TFIDF: TFIDF(tf) size: " + TFIDF.size());
            normalizeTFIDF(TFIDF);
            logger.info("TFIDF: normalize done.");
            writeTFIDF(ctx.getKVS(), TFIDF);
            logger.info("TFIDF: write TFIDF done.");
			ctx.output("OK");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			logger.error(e.getMessage());
		}
	}
 
	public static ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> updateIDF(KVSClient client, FlamePairRDD IDF) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> TFIDF = new ConcurrentHashMap<>();
		try {
			Iterator<Row> iter = client.scan(IDF.getName());
			while (iter.hasNext()) {
				Row row = iter.next();
                String word = row.key();
				for (String col : row.columns()) {
                    String[] str = row.get(col).split(separator);
                    String url = str[0];
                    double idf = Double.valueOf(str[1]);
                    if (!TFIDF.containsKey(url)) {
                        TFIDF.put(url, new ConcurrentHashMap<>());
                    }
                    TFIDF.get(url).put(word,  idf);
                }
			}
		} catch (Exception e) {
            logger.error("err updating idf: " + e.getMessage());
        }
        return TFIDF;
	}

	public static void updateTF(KVSClient client, ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> TFIDF, FlamePairRDD TF) {
		try {
			Iterator<Row> iter = client.scan(TF.getName());
			while (iter.hasNext()) {
				Row row = iter.next();
                String url = row.key();
				for (String col : row.columns()) {
                    String[] str = row.get(col).split(separator);
                    String word = str[0];
                    double tf = Double.valueOf(str[1]);
                    if (!TFIDF.containsKey(url)) {
                        logger.error("TFIDF: not contain url: " + url);
                        continue;
                    }
                    if (!TFIDF.get(url).containsKey(word)) {
                        logger.error("TFIDF: url=" + url + " not contain word=" + word);
                        continue;
                    }
                    TFIDF.get(url).put(word, TFIDF.get(url).get(word) * tf);
                }
			}
		} catch (Exception e) {
            logger.error("err updating tf: " + e.getMessage());
        }
	}

    public static void normalizeTFIDF(ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> TFIDF) {
        for (String url : TFIDF.keySet()) {
            double norm = 0.0;
            ConcurrentHashMap<String, Double> scores = TFIDF.get(url);
            for (String word : scores.keySet()) {
                norm += Math.pow(scores.get(word), 2);
            }
            norm = Math.sqrt(norm);
            for (String word : scores.keySet()) {
                scores.put(word, scores.get(word) / norm);
            }
        }
    }

    public static void writeTFIDF(KVSClient client, ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> TFIDF) {
        try {
            client.persist("tfidf");
			ByteArrayOutputStream out = new ByteArrayOutputStream();
            int cnt = 0;
            for (String url : TFIDF.keySet()) {
				Row row = new Row(url);
                ConcurrentHashMap<String, Double> scores = TFIDF.get(url);
				for (String word : scores.keySet()) {
					double score = scores.get(word);
					row.put(word, ""+score);
				}
				out.write(row.toByteArray());
				out.write("\n".getBytes());
                cnt++;
                if (cnt == 500) {
                    out.close();
                    client.putRows("tfidf", out.toByteArray());
                    out = new ByteArrayOutputStream();
                    cnt = 0;
                }
			}
            out.close();
			client.putRows("tfidf", out.toByteArray());
		} catch (Exception e) {
            logger.error("err writing TFIDF: " + e.getMessage());
        }
	}
}
