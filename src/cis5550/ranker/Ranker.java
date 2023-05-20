package cis5550.ranker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;

import static cis5550.webserver.Server.*;
import cis5550.frontend.ResultItem;
import com.google.gson.*;
import cis5550.flame.*;
import cis5550.tools.Stemmer;
import cis5550.tools.*;
import cis5550.kvs.*;

public class Ranker {
	private static final Logger logger = Logger.getLogger(Ranker.class);
    static ConcurrentHashMap<String, ConcurrentHashMap<String, Hit>> hitMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> TFIDF = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> urlIndex = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> titleIndex = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, Double> pageranks = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, String> titles = new ConcurrentHashMap<>();
    static PriorityQueue<Row> pq = new PriorityQueue<Row>(new Comparator<Row>(){
	   public int compare(Row row1, Row row2){
	       if(Double.valueOf(row2.get("rank"))>Double.valueOf(row1.get("rank"))){
	          return -1;
	       }
	       else{
	          return 1;
	       }
	   }
	});
    //    static ConcurrentHashMap<String, String> hash2url = new ConcurrentHashMap<>();
    static HashSet<String> stopwords = new HashSet<>();
    static final double tf_coef = 0.4;
    static final double tfidf_max = 0.15;
    static final double pagerank_weight = 0.1;
    static final double pagerank_max = 2.0;
    static final double stem_boost = 0.1;
    static final double wordHit_boost = 3.0;
    static final double titleHit_boost = 0.15;
    static final double urlHit_boost = 0.15;
    static int numReturn = 50;
    static boolean isDebugMode = false;
	private static final String separator = ",";
	private static final String maxCountFlag = "##max##";
	
    static class SuggestItem{
        private String url = "";
    	private String title = "";
    	private double pagerank = 0;
        public SuggestItem(String url, String title, double pagerank) {
        	this.url = url;
    		this.title = title;
    		this.pagerank = pagerank;
    	}
    }

	public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            logger.error("Ranking server: Wrong number of arguments: <port> <kvsMasterIP:port> <optional:isDebugMode>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        String kvsMaster = args[1];
        if (args.length > 2) {
            isDebugMode = args[2].equals("true") ? true : false;
        }
        port(port);
        logger.info("Ranking server listen at port: " + port);
        System.out.println("Ranking server listen at port: " + port);
        KVSClient client = new KVSClient(kvsMaster);
        logger.info("connect to kvs master: " + kvsMaster);
    	System.out.println("connect to kvs master: " + kvsMaster);

        loadTFIDF(client);
        logger.info("ranker: TFIDF size: " + TFIDF.size());
    	System.out.println("TFIDF size: " + TFIDF.size());

        loadURLIndex(client);
        logger.info("ranker: url index size: " + urlIndex.size());
    	System.out.println("ranker: url index size: " + urlIndex.size());

        loadTitleIndex(client);
        logger.info("ranker: title index size: " + titleIndex.size());
    	System.out.println("ranker: title index size: " + titleIndex.size());

        // loadInverseIndex(client, hitMap.size());
        loadTitles(client);
        logger.info("ranker: loaded titles.");
    	System.out.println("ranker: loaded titles.");

        loadPageRanks(client);
        logger.info("ranker: pageranks size: " + pageranks.size());
    	System.out.println("ranker: pageranks size: " + pageranks.size());

        loadStopwords("stopwords.txt");
        logger.info("ranker: stopwords size: " + stopwords.size());
    	System.out.println("ranker: stopwords size: " + stopwords.size());

//        loadHash2URL("hash2url.txt");
//        logger.info("ranker: loaded hash2url.");
        
        logger.info("ranker: start serving..");
        
    	Gson gson = new Gson();

    	Vector<SuggestItem> sugItems = new Vector<>();
    	for(int i = numReturn - 1; i >= 0; --i) {
        	Row curRow = pq.poll();
        	String url = curRow.key();
        	
            String realURL = new String(client.get("crawl",url,"url"), StandardCharsets.UTF_8);
//            String urltitle = new String(client.get("titles",url,"title"), StandardCharsets.UTF_8);
            double pagerank = Double.valueOf(curRow.get("rank"));
//            System.out.println("Pagerank: "+pagerank+", title: "+titles.get(url));
        	SuggestItem suggest = new SuggestItem(realURL, titles.get(url),pagerank);
        	sugItems.add(suggest);
        }
    	
        String trending = gson.toJson(sugItems);
        System.out.println("ranker: start serving..");

	    get("/suggest", (req, res) -> {
	        res.header("Access-Control-Allow-Origin", "*");
	    	res.header("Content-Type","application/json");
            return trending;
	    	
	    });

        get("/search/:term", (req, rsp) -> {
	        rsp.header("Access-Control-Allow-Origin", "*");
            rsp.type("application/json");
            String query = req.params("term");
            // String query = req.queryParams("q");
            logger.info("processing query:" + query);
            Vector<String> words = filterQuery(query);
            logger.info("filtered query:" + String.join(" ", words));
            if (words.size() == 0)
                return gson.toJson(words);
            HashMap<String, Double> queryScores = new HashMap<>();
            double maxCount = 0.0;
            for (String word : words) {
                queryScores.put(word, queryScores.getOrDefault(word, 0.0) + 1);
                maxCount = Math.max(maxCount, queryScores.get(word));
                String stemWord = getStemWord(word);
                if (!word.equals(stemWord)) {
                    queryScores.put(stemWord, queryScores.getOrDefault(stemWord, 0.0) + 1);
                    maxCount = Math.max(maxCount, queryScores.get(stemWord));
                }
            }
            HashSet<String> urlSet = new HashSet<>();
            for (String word : queryScores.keySet()) {
                queryScores.put(word, queryScores.get(word) / maxCount);
                updateIDF(queryScores, client, word, urlSet);
            }
            for (String word : words) {
                String stemWord = getStemWord(word);
                if (queryScores.containsKey(stemWord)) {
                    queryScores.put(stemWord, queryScores.get(stemWord) * stem_boost);
                }
            }
            normalizeScores(queryScores);
            Vector<urlTuple> candidates = computeurlScores(queryScores, urlSet);
            candidates.sort(new Comparator<urlTuple>() {
                @Override
                public int compare(urlTuple x1, urlTuple x2) {
                    return x1.score - x2.score >= 0.0 ? -1 : 1; // high score put at front
                }
            });
            Vector<ResultItem> resItems = new Vector<>();
            String debugRec = "";
            int cnt = 0;
            HashSet<String> retURLSet = new HashSet<>();
            for (int i=0; i<candidates.size(); i++) {
                String url = candidates.get(i).url;
                double score = candidates.get(i).score;
                String hitInfo = "";
                if (isDebugMode) {
                    for (String word : words) {
                        String stemWord = getStemWord(word);
                        if (TFIDF.containsKey(url) && TFIDF.get(url).containsKey(word)) {
                            double tfidf = TFIDF.get(url).get(word);
                            hitInfo += (word + " tfidf=" + round(tfidf, 3)) + ";";
                        }
                        if (TFIDF.containsKey(url) && TFIDF.get(url).containsKey(stemWord)) {
                            double tfidf = TFIDF.get(url).get(stemWord);
                            hitInfo += ("(sw)" + stemWord + " tfidf=" + round(tfidf, 3)) + ";";
                        }
                        if (urlIndex.containsKey(url) && urlIndex.get(url).containsKey(word)) {
                            double urlHit = urlIndex.get(url).get(word);
                            hitInfo += (word + " urlHit=" + round(urlHit, 3)) + ";";
                        }
                        if (urlIndex.containsKey(url) && urlIndex.get(url).containsKey(stemWord)) {
                            double urlHit = urlIndex.get(url).get(stemWord);
                            hitInfo += ("(sw)" + stemWord + " urlHit=" + round(urlHit, 3)) + ";";
                        }
                        if (titleIndex.containsKey(url) && titleIndex.get(url).containsKey(word)) {
                            double titleHit = titleIndex.get(url).get(word);
                            hitInfo += (word + " titleHit=" + round(titleHit, 3)) + ";";
                        }
                        if (titleIndex.containsKey(url) && titleIndex.get(url).containsKey(stemWord)) {
                            double titleHit = titleIndex.get(url).get(stemWord);
                            hitInfo += ("(sw)" + stemWord + " titleHit=" + round(titleHit, 3)) + ";";
                        }
                    }
                }
//                String realURL = hash2url.get(url);
//                ResultItem item = new ResultItem(realURL, i+1, round(score, 3), round(pageranks.getOrDefault(url, 0.0), 3), hitInfo);
                String realURL = new String(client.get("crawl",url,"url"), StandardCharsets.UTF_8);
                byte[] title = client.get("titles",url,"title");
                String urltitle = (title == null) ? realURL : new String(title, StandardCharsets.UTF_8);

                if (!urltitle.replaceAll("\\p{Punct}", "").replaceAll("[^\\p{L}^\\d]", "").matches("[a-zA-Z0-9\\p{Punct}\\s]*")) {
                    continue;
                }
                String[] parse = parseURL(realURL);
                String urlClip = parse[0] + "://" + parse[1] + ":" + parse[2] + parse[3].split("\\?", 2)[0];
                if (retURLSet.contains(urlClip)) {
                    continue;
                }
                retURLSet.add(urlClip);

                ResultItem item = new ResultItem(realURL, i+1, round(score, 3), round(pageranks.getOrDefault(url, 0.0), 3), hitInfo, urltitle, url);
                
                resItems.add(item);
                debugRec = debugRec.isEmpty() ? realURL : (debugRec + "," + realURL);

                cnt++;
                if (cnt == numReturn) {
                    break;
                }
            }
            if (isDebugMode) {
                logger.debug("recommend " + candidates.size() + " candidates:\n" + debugRec);
            }
//            Gson gson = new Gson();
            String json = gson.toJson(resItems); 
            // rsp.status(200, "OK");
            // rsp.header("Content-Length", String.valueOf(html.length()));
            // rsp.body(json);
            return json;
        });
	}

    static class Hit {
        public int count;
        public boolean isCapitalize;
        public String position;
        public double tf;
        public double idf;
    }

    static class urlTuple {
        public String url;
        public double score;
        urlTuple(String url, double score) {
            this.url = url;
            this.score = score;
        }
    }

    public static Hit loadHit(Row row, String word) {
        Hit hit = new Hit();
        // infoLst: wordCnt wordPos_1 wordPos_2 ... wordPos_cnt
        String[] infoLst = row.get(word).split(separator);
        int count = Integer.valueOf(infoLst[0]);
        hit.count = count;
        Vector<String> pos = new Vector<>();
        try {
            for (int i=0; i<count; i++) {
                pos.add(infoLst[i+1]);
            }
        } catch (Exception e) {
            if (!word.equals(maxCountFlag)) {
                logger.error(e.getMessage());
                throw e;
            }
        }
        hit.position = String.join(separator, pos);
        double tf =  tf_coef + (1-tf_coef) * (double)count / Double.valueOf(row.get("##max##"));
        hit.tf = tf;
        return hit;
    }

    public static String getStemWord(String word) {
        Stemmer s = new Stemmer();
        s.add(word.toCharArray(), word.length());
        s.stem();
        return s.toString();
    }
    
//    public static void loadHash2URL(String fileName) {
//        try {
//            File f = new File(fileName);
//            if (!f.exists()) {
//                logger.error("hash2url.txt not exist.");
//                System.exit(1);
//            }
//            BufferedReader br = new BufferedReader(new FileReader(f));
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] str = line.split(separator);
//                String hash = str[0];
//                String url = str[1];
//                hash2url.put(hash, url);
//            }
//            br.close();
//        } catch (Exception e) {
//            logger.error("loading hash2url.txt err: " + e.getMessage());
//        }
//    }
    
    public static void loadTitles(KVSClient client) {
        try {
            Iterator<Row> iter = client.scan("titles");
            while(iter.hasNext()) {
            	 Row row = iter.next();
                 String url = row.key();
                 titles.put(url, row.get("title"));
            }
        } catch (Exception e) {
            logger.error("err loading titles: " + e.getMessage());
        }
    }
    
    public static void loadForwardIndex(KVSClient client) {
        try {
            Iterator<Row> iter = client.scan("forwardIndex");
            while(iter.hasNext()) {
                Row row = iter.next();
                String url = row.key();
                for (String word : row.columns()) {
                    Hit hit = loadHit(row, word);
                    if (!hitMap.containsKey(url)) {
                        hitMap.put(url, new ConcurrentHashMap<>());
                    }
                    hitMap.get(url).put(word, hit);
                }
            }
        } catch (Exception e) {
            logger.error("err loading forward index: " + e.getMessage());
        }
    }

    public static void loadTFIDF(KVSClient client) {
        try {
            Iterator<Row> iter = client.scan("tfidf");
            int cnt = 0;
            while(iter.hasNext()) {
                Row row = iter.next();
                String url = row.key();
                for (String word : row.columns()) {
                    if (!TFIDF.containsKey(url)) {
                        TFIDF.put(url, new ConcurrentHashMap<>());
                    }
                    TFIDF.get(url).put(word, Math.min(tfidf_max, Double.valueOf(row.get(word))));
                }
                cnt++;
                if (cnt % 1000 == 0) {
                    logger.debug("loading tfidf number: " + cnt);
                }
            }
        } catch (Exception e) {
        	System.out.println("err loading TFIDF: " + e.getMessage());
            logger.error("err loading TFIDF: " + e.getMessage());
        }
    }

    public static void loadURLIndex(KVSClient client) {
        try {
            Iterator<Row> iter = client.scan("urlIndex");
            while(iter.hasNext()) {
                Row row = iter.next();
                String url = row.key();
                for (String word : row.columns()) {
                    if (!urlIndex.containsKey(url)) {
                        urlIndex.put(url, new ConcurrentHashMap<>());
                    }
                    urlIndex.get(url).put(word, Double.valueOf(row.get(word).split(separator, 2)[0]));
                }
            }
        } catch (Exception e) {
        	System.out.println("err loading TFIDF: " + e.getMessage());
            logger.error("err loading TFIDF: " + e.getMessage());
        }
    }

    public static void loadTitleIndex(KVSClient client) {
        try {
            Iterator<Row> iter = client.scan("titleIndex");
            while(iter.hasNext()) {
                Row row = iter.next();
                String url = row.key();
                for (String word : row.columns()) {
                    if (!titleIndex.containsKey(url)) {
                        titleIndex.put(url, new ConcurrentHashMap<>());
                    }
                    titleIndex.get(url).put(word, Double.valueOf(row.get(word).split(separator, 2)[0]));
                }
            }
        } catch (Exception e) {
        	System.out.println("err loading TFIDF: " + e.getMessage());
            logger.error("err loading TFIDF: " + e.getMessage());
        }
    }

    public static void loadInverseIndex(KVSClient client, int numurl) {
        try {
            Iterator<Row> iter = client.scan("inverseIndex");
            while(iter.hasNext()) {
                Row row = iter.next();
                String word = row.key();
                for (String col : row.columns()) {
                    String[] urls = row.get(col).split(separator);
                    for (String url : urls) {
                        if (!hitMap.containsKey(url)) {
                            logger.debug("url not in fIndex: word=" + word + ", url=" + url);
                            continue;
                        }
                        if (!hitMap.get(url).containsKey(word)) {
                            logger.debug("word not in fIndex: word=" + word + ", url=" + url);
                            continue;
                        }
                        hitMap.get(url).get(word).idf = Math.log((double)numurl / urls.length);
                        if (isDebugMode) {
                            Hit hit = hitMap.get(url).get(word);
                            client.put("hitMap", url, word, hit.count+";"+hit.position+";"+hit.tf+";"+hit.idf);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("err loading inverse index: " + e.getMessage());
        }
    }

    public static void loadPageRanks(KVSClient client) {
        try {
            Iterator<Row> iter = client.scan("pageranks");
            while(iter.hasNext()) {
                Row row = iter.next();
                String url = row.key();
                pageranks.put(url, Math.log(1 + Double.valueOf(row.get("rank"))));
                // pageranks.put(url, Math.min(pagerank_max, Double.valueOf(row.get("rank"))));
                if(titles.containsKey(url) && !titles.get(url).startsWith("http") && !titles.get(url).equals("Untitled")) {
//                	System.out.println(titles.get(url));
                	Row newRow = new Row(url);
                	String pr = Math.log(1 + Double.valueOf(row.get("rank")))+"";                	
                	newRow.put("rank",pr);
                	newRow.put("title", titles.get(url));
                	pq.add(newRow);
                    if (pq.size() > numReturn) pq.poll();    
                }
            }
        } catch (Exception e) {
        	System.out.println("err loading pageranks: " + e.getMessage());
            logger.error("err loading pageranks: " + e.getMessage());
        }
    }
    
    public static void loadStopwords(String stopwordsFile) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(stopwordsFile));
			String word;
			while((word = br.readLine()) != null) {
				stopwords.add(word);
				Stemmer s = new Stemmer();
				s.add(word.toCharArray(), word.length());
				s.stem();
				stopwords.add(s.toString());
			}
			br.close();
		} catch (Exception e) {logger.error("loading stopwords err: " + e.getMessage());}
	}

    public static Vector<String> filterQuery(String query) {
        Vector<String> words = new Vector<>();
        for (String w : query.toLowerCase().split(" ")) {
            if (!stopwords.contains(w))
                words.add(w);
        }
        return words;
    }

    public static double round(double value, int decimal) {
        if (decimal < 0) throw new IllegalArgumentException();
        long factor = (long) Math.pow(10, decimal);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    public static void updateWordurlScores(HashMap<String, Double> scores, KVSClient client, String word, double boost) {
        try {
            Row row = client.getRow("inverseIndex", word);
            if (row != null) {
                for (String col : row.columns()) {
                    String[] urls = row.get(col).split(",");
                    int urlCount = 0;
                    for (String url : urls) {
                        if (!hitMap.containsKey(url) || !hitMap.get(url).containsKey(word)) {
                            logger.debug("absent url: " + url + ", word: " + word);
                            continue;
                        }
                        if (hitMap.get(url).get(word).idf > -1.0)
                            break; // idf computed for this word
						urlCount++;
                    }
                    if (urlCount == 0) {
                        logger.debug("word: " + word + ", urls length=" + urls.length + ", idf details: " + hitMap.size() + ", urlCount="+urlCount);
                    }
                    double idf = urlCount == 0 ? 1.0 : Math.log((double)hitMap.size() / urlCount);
                    for (String url : urls) {
                        if (!hitMap.containsKey(url))
                            continue;
                        Hit hit = hitMap.get(url).get(word);
                        hit.idf = hit.idf > -1.0 ? hit.idf : idf;
                        double score = hit.tf * hit.idf;
                        scores.put(url, scores.getOrDefault(url, 1.0) * score * boost);
                    }
                }
            }
        } catch (Exception e) {logger.error(e.getMessage());}
    }

    public static void updateIDF(HashMap<String, Double> queryScores, KVSClient client, String word, HashSet<String> urlSet) {
        try {
            Row row = client.getRow("inverseIndex", word);
            if (row != null) {
                for (String col : row.columns()) {
                    String[] urls = row.get(col).split(",");
                    double idf = urls.length == 0 ? 0.0 : Math.log((double)TFIDF.size() / urls.length);
                    queryScores.put(word, queryScores.get(word) * idf);
                    for (String url : urls) {
                        urlSet.add(url);
                    }
                }
            } else {
                queryScores.put(word, 0.0);
            }
        } catch (Exception e) {logger.error(e.getMessage());}
    }

    public static void normalizeScores(HashMap<String, Double> scores) {
        double norm = 0.0;
        for (String word : scores.keySet()) {
            norm += Math.pow(scores.get(word), 2);
        }
        norm = Math.sqrt(norm);
        for (String word : scores.keySet()) {
            scores.put(word, scores.get(word) / norm);
        }
    }

    public static Vector<urlTuple> computeurlScores(HashMap<String, Double> queryScores, HashSet<String> urlSet) {
        Vector<urlTuple> candidates = new Vector<>();
        for (String url : urlSet) {
            double tfidf = 0.0;
            int wordHit = 0;
            if (TFIDF.containsKey(url)) {
                ConcurrentHashMap<String, Double> urlScores = TFIDF.get(url);
                for (String word : queryScores.keySet()) {
                    tfidf += (queryScores.get(word) * urlScores.getOrDefault(word, 0.0));
                    if (urlScores.containsKey(word))
                        wordHit++;
                }
            }
            double titleHit = 0.0;
            ConcurrentHashMap<String, Double> titleScores = titleIndex.getOrDefault(url, new ConcurrentHashMap<>());
            for (String word : queryScores.keySet()) {
                // titleHit += titleScores.getOrDefault(word, 0.0);
                if (titleScores.containsKey(word)) {
                    titleHit += titleScores.getOrDefault(word, 0.0);
                    // titleHit++;
                }
            }
            double urlHit = 0.0;
            ConcurrentHashMap<String, Double> urlScores = urlIndex.getOrDefault(url, new ConcurrentHashMap<>());
            for (String word : queryScores.keySet()) {
                // urlHit += urlScores.getOrDefault(word, 0.0);
                if (urlScores.containsKey(word) && !titleScores.containsKey(word)) {
                    urlHit += urlScores.getOrDefault(word, 0.0);
                    // urlHit++;
                }
            }
            double totalScore = (wordHit + wordHit_boost * (wordHit - 1)) * tfidf;
            totalScore += (pagerank_weight * pageranks.getOrDefault(url, 0.0));
            totalScore += titleHit_boost * titleHit;
            // totalScore += (titleHit_boost * Math.min(titleHit, 1.0));
            totalScore += urlHit_boost * urlHit;
            // totalScore += (urlHit_boost * Math.min(urlHit, 1.0));
            urlTuple ut = new urlTuple(url, totalScore);
            candidates.add(ut);
        }
        return candidates;
    }

    public static String[] parseURL(String url) {
        String result[] = new String[4];
        int slashslash = url.indexOf("//");
        if (slashslash>0) {
          result[0] = url.substring(0, slashslash-1);
          int nextslash = url.indexOf('/', slashslash+2);
          if (nextslash>=0) {
            result[1] = url.substring(slashslash+2, nextslash);
            result[3] = url.substring(nextslash);
          } else {
            result[1] = url.substring(slashslash+2);
            result[3] = "/";
          }
          int colonPos = result[1].indexOf(':');
          if (colonPos > 0) {
            result[2] = result[1].substring(colonPos+1);
            result[1] = result[1].substring(0, colonPos);
          }
        } else {
          result[3] = url;
        }
    
        return result;
      }
}