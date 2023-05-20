package cis5550.jobs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.flame.*;
import cis5550.tools.Stemmer;
import cis5550.tools.*;
import cis5550.kvs.*;

public class InverseIndexer {
	private static final Logger logger = Logger.getLogger(InverseIndexer.class);
	private static final String separator = ",";
	private static final String maxCountFlag = "##max##";

	public static void run(FlameContext ctx, String[] urlArg) throws Exception {
		try {
			HashSet<String> lexicon = loadLexicon("lexicon.txt", "words.txt");
			HashSet<String> stopwords = loadStopwords("stopwords.txt");
			logger.info("inverse indexer: lexicon size: " + lexicon.size());
			logger.info("inverse indexer: stopwords size: " + stopwords.size());
			logger.info("inverse indexer: num workers: " + ctx.getKVS().numWorkers());

			FlameRDD rdd = ctx.fromTable("crawl", row -> Hasher.hash(row.get("url")) + separator + row.get("page"));

			FlamePairRDD pairRDD = rdd.mapToPair(s -> {
				String[] str = s.split(separator, 2);
				return new FlamePair(str[0], str[1]);
			});
			ctx.getKVS().delete(rdd.getName());
			FlamePairRDD invIndexExec = pairRDD.flatMapToPair(pair -> {
				String url = pair._1();
				String page = pair._2();
				List<FlamePair> wuPairLst = new ArrayList<FlamePair>();

				// replace HTML tags
				String text = page.replaceAll("<[^>]*>", " ");
				// Remove punctuation and convert to lower case
				text = text.replaceAll("\\p{Punct}", " ").replaceAll("[^\\p{L}^\\d]", " ").toLowerCase();
				// Split into words and remove duplicates
				text = text.replaceAll("[\\r\\n\\t]", " ").replaceAll("\\s+", " ").trim();
				List<String> words = Arrays.asList(text.split(" "));
				HashSet<String> uniqueWords = new HashSet<>();
				for (String word : words) {
					if (!stopwords.contains(word) && lexicon.contains(word))
						uniqueWords.add(word);
				}
				List<String> stemmedWords = new ArrayList<String>();
				for (String word : uniqueWords) {
					String stemWord = getStemWord(word);
					if (!stopwords.contains(word) && lexicon.contains(word))
						stemmedWords.add(stemWord);
				}
				for (String sw : stemmedWords) {
					uniqueWords.add(sw);
				}
				// Iterate over unique words
				for (String word : uniqueWords) {
					if (word.length() > 0) {
						FlamePair wuPair = new FlamePair(word, url);
						wuPairLst.add(wuPair);
					}
				}
				Iterable<FlamePair> iter = wuPairLst;
				return iter;
			});
			ctx.getKVS().delete(pairRDD.getName());
			FlamePairRDD invIndex = invIndexExec.foldByKey("", (s1, s2) -> {
				return (s1.equals("")) ? s2 : s1 + separator + s2;
			});
			ctx.getKVS().delete(invIndexExec.getName());
			writeInvIndex(ctx.getKVS(), invIndex);
			System.out.println("inverse indexer: inverseIndex size: " + ctx.getKVS().count("inverseIndex"));
			logger.info("inverse indexer: inverseIndex size: " + ctx.getKVS().count("inverseIndex"));
			ctx.getKVS().delete(invIndex.getName());

			ctx.output("OK");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			logger.error(e.getMessage());
		}
	}

	public static Vector<String> loadIDurl(String filename) {
		File f = new File(filename);
		Vector<String> id2url = new Vector<>();
		try {
			if (f.exists()) {
				BufferedReader br = new BufferedReader(new FileReader(f));
				String line;
				while ((line = br.readLine()) != null) {
					id2url.add(line);
				}
				br.close();
			}
		} catch (Exception e) {
			logger.error("loading id2url err: " + e.getMessage());
		}
		return id2url;
	}

	public static String getStemWord(String word) {
		Stemmer s = new Stemmer();
		s.add(word.toCharArray(), word.length());
		s.stem();
		return s.toString();
	}

	public static ConcurrentHashMap<String, String> getURL2ID(Vector<String> idurl) {
		ConcurrentHashMap<String, String> url2id = new ConcurrentHashMap<>();
		for (String x : idurl) {
			String[] split = x.split(",", 2);
			url2id.put(split[1], split[0]);
		}
		return url2id;
	}

	public static HashSet<String> loadLexicon(String lexiconFile, String rawWordFile) {
		HashSet<String> lexicon = new HashSet<>();
		File f = new File(lexiconFile);
		try {
			if (!f.exists()) {
				BufferedReader br = new BufferedReader(new FileReader(rawWordFile));
				String word;
				while ((word = br.readLine()) != null) {
					word = word.toLowerCase();
					lexicon.add(word);
					lexicon.add(getStemWord(word));
				}
				br.close();
				BufferedWriter bw = new BufferedWriter(new FileWriter(f, false));
				Vector<String> lexiconVec = new Vector<>();
				for (String w : lexicon) {
					lexiconVec.add(w);
				}
				lexiconVec.sort(null);
				lexiconVec.forEach(x -> {
					try {
						bw.write(x);
						bw.newLine();
					} catch (Exception e) {
						logger.error("write err: " + e.getMessage());
					}
				});
				bw.close();
			} else {
				BufferedReader br = new BufferedReader(new FileReader(lexiconFile));
				String w;
				while ((w = br.readLine()) != null) {
					lexicon.add(w);
				}
				br.close();
			}
		} catch (Exception e) {
			logger.error("loading lexicon err: " + e.getMessage());
		}
		return lexicon;
	}

	public static HashSet<String> loadStopwords(String stopwordsFile) {
		HashSet<String> stopwords = new HashSet<>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(stopwordsFile));
			String word;
			while ((word = br.readLine()) != null) {
				stopwords.add(word);
				stopwords.add(getStemWord(word));
			}
			br.close();
		} catch (Exception e) {
			logger.error("loading stopwords err: " + e.getMessage());
		}
		return stopwords;
	}

	public static void writeInvIndex(KVSClient client, FlamePairRDD invIndex) {
		try {
			client.persist("inverseIndex");
			Iterator<Row> iter = client.scan(invIndex.getName());
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			int cnt = 0;
			while (iter.hasNext()) {
				Row row = iter.next();
				out.write(row.toByteArray());
				out.write("\n".getBytes());
                cnt++;
                if (cnt == 500) {
                    out.close();
                    client.putRows("inverseIndex", out.toByteArray());
                    out = new ByteArrayOutputStream();
                    cnt = 0;
                }
			}
			out.close();
			client.putRows("inverseIndex", out.toByteArray());
		} catch (Exception e) {
			logger.error("err writing inverse index: " + e.getMessage());
		}
	}
}