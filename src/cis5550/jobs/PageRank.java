package cis5550.jobs;

import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

public class PageRank {
	private static final Logger logger = Logger.getLogger(PageRank.class);
	private static final String seperator = ",";

	public static void run(FlameContext ctx, String[] urlArg) throws Exception {
//		FlameRDD rdd = ctx.fromTable("crawl", row -> row.get("url") + seperator + row.get("page"));
		FlameRDD rdd = ctx.fromTable("crawl", row -> row.get("url"));
		Double threshold = ((urlArg.length == 0) ? (double) 0.01 : Double.parseDouble(urlArg[0]));
		Double decayFactor = 0.85;
		String hostAndPort = ctx.getKVS().getMaster();

		// EC2: enhanced convergence criterion
		String convergence = "";
		try {
			convergence = urlArg[1];
		} catch (Exception e) {
		}
		final String c = convergence;

		// stable table: (url, 1.0,1.0,outUrl)
		logger.info("pagerank: crawl table size: " + rdd.count());

		FlamePairRDD stablePairRDD = rdd.mapToPair(s -> {
//			String[] str = s.split(seperator, 2);
//			LinkedList<String> extractedURL = extractURL(str[1]);
//			LinkedList<String> normalizedURL = normalizeURL(extractedURL, str[0]);
			KVSClient kvs = new KVSClient(hostAndPort);
			String urlKey = Hasher.hash(s);
			String page = "";
			if(kvs.get("crawl",urlKey,"page") != null) page = new String(kvs.get("crawl",urlKey,"page"));
//			System.out.println("page: "+page);
			LinkedList<String> extractedURL = extractURL(page);
			LinkedList<String> normalizedURL = normalizeURL(extractedURL,s);
			Set<String> uniqueOutURL = new HashSet<>(normalizedURL);
			String urlLst = "";
			for (String u : uniqueOutURL) {
				String hashedUrl = Hasher.hash(u);
				urlLst = (urlLst.equals("")) ? hashedUrl : urlLst + seperator + hashedUrl;
			}
			return new FlamePair(urlKey, "1.0,1.0," + urlLst);
		});
		ctx.getKVS().delete(rdd.getName());
//		stablePairRDD.saveAsTable("stable");
		System.out.println("stable1 size=" + stablePairRDD.count());
		logger.info("pagerank: stablePairRDD size: " + stablePairRDD.count());
		System.out.println("pagerank: iteration starts.");
		logger.info("pagerank: iteration starts.");

		int iteration = 0;
		while (true) {
			System.out.println("#### Iteration: " + iteration);
			// transfer table input: pair (url, "currVal, prevVal, allOutURL"
			FlamePairRDD transferPairRDD = stablePairRDD.flatMapToPair(pair -> {
				String url = pair._1();
				String valLst = pair._2();
				String[] vals = valLst.split(",", 3);
				String[] outURLLst = vals[2].split(seperator);
				Set<String> uniqueOutURL = new HashSet<>(Arrays.asList(outURLLst));
				List<FlamePair> lvPairLst = new ArrayList<FlamePair>();
				lvPairLst.add(new FlamePair(url, 0.0 + ""));

				KVSClient kvs = new KVSClient(hostAndPort);

				for (String outURL : uniqueOutURL) {
					// if it has outURL
					if (!outURL.equals("")) {
						if(kvs.existsRow("crawl",outURL)) {
							Double rc = Double.parseDouble(vals[0]);
							FlamePair lv = new FlamePair(outURL, String.valueOf(decayFactor * rc / uniqueOutURL.size()));
							lvPairLst.add(lv);
						}
					}
				}

				Iterable<FlamePair> iter = lvPairLst;
				return iter;
			});
			transferPairRDD.saveAsTable("transferPairRDD" + "-" + iteration);
			System.out.println("pagerank: transferPairRDD size: " + transferPairRDD.count());
			logger.info("pagerank: transferPairRDD size: " + transferPairRDD.count());

			// aggregate the transfer: input (url, new page rank val)
			FlamePairRDD aggTransferPairRDD = transferPairRDD.foldByKey("0.0", (s1, s2) -> {
				Double answer = Double.parseDouble(s1) + Double.parseDouble(s2);
				return String.valueOf(answer);
			});
			ctx.getKVS().delete(transferPairRDD.getName());
			aggTransferPairRDD.saveAsTable("aggTransferPairRDD" + "-" + iteration);
			System.out.println("pagerank: aggTransferPairRDD size: " + aggTransferPairRDD.count());
			logger.info("pagerank: aggTransferPairRDD size: " + aggTransferPairRDD.count());

			// update the state table
			FlamePairRDD joinPairRDD = stablePairRDD.join(aggTransferPairRDD);
			ctx.getKVS().delete(stablePairRDD.getName());
			ctx.getKVS().delete(aggTransferPairRDD.getName());
			joinPairRDD.saveAsTable("joinPairRDD" + "-" + iteration);
			System.out.println("pagerank: joinPairRDD size: " + joinPairRDD.count());
			logger.info("pagerank: joinPairRDD size: " + joinPairRDD.count());

			FlamePairRDD updatedPairRDD = joinPairRDD.flatMapToPair(pair -> {
				String url = pair._1();
				String valLst = pair._2();
				// totalSplit: currVal, prevVal, url1, url2..., updatedVal
				String[] totalSplit = valLst.split(",");
				String currRank = (1 - decayFactor) + Double.parseDouble(totalSplit[totalSplit.length - 1]) + "";
				String prevRank = totalSplit[0];
				String urlSplit = valLst.split(",", 3)[2];
				String outURL = urlSplit.substring(0, urlSplit.lastIndexOf(","));
				String newVal = currRank + "," + prevRank + "," + outURL;
				List<FlamePair> pairLst = new ArrayList<FlamePair>();
				pairLst.add(new FlamePair(url, newVal));
				Iterable<FlamePair> iter = pairLst;
				return iter;
			});
			ctx.getKVS().delete(joinPairRDD.getName());
			System.out.println("pagerank: updatedPairRDD size: " + updatedPairRDD.count());
			logger.info("pagerank: updatedPairRDD size: " + updatedPairRDD.count());

			FlameRDD updateDiff = updatedPairRDD.flatMap(pair -> {
				String url = pair._1();
				String valLst = pair._2();
				String[] vals = valLst.split(",");
				Double currRank = Double.parseDouble(vals[0]);
				Double prevRank = Double.parseDouble(vals[1]);
				Double absDiff = Math.abs(currRank - prevRank);
				List<String> pairLst = new ArrayList<String>();
				pairLst.add(url + seperator + absDiff);
				Iterable<String> iter = pairLst;
				return iter;
			});
			updateDiff.saveAsTable("updateDiff" + "-" + iteration);
			System.out.println("pagerank: updateDiff size: " + updateDiff.count());
			logger.info("pagerank: updateDiff size: " + updateDiff.count());
			String maxURL = updateDiff.fold("/" + seperator + "0.0", (s1, s2) -> {
				Double v1 = Double.parseDouble(s1.split(seperator)[1]);
				Double v2 = Double.parseDouble(s2.split(seperator)[1]);
				String output = (v1 >= v2) ? s1 : s2;
				return output;
			});
			String maxDiff = maxURL.split(seperator)[1];
			iteration += 1;

			if (c.equals(""))
				ctx.getKVS().delete(updateDiff.getName());
			stablePairRDD = updatedPairRDD;
			
			System.out.println("stable2 size=" + stablePairRDD.count());

			if (!c.equals("")) {
				Double conv = Double.parseDouble(c) / 100;
				Double min_url_conv = updateDiff.count() * conv;
				Vector<String> url_diff = updateDiff.take(updateDiff.count() + 1);

				int num_url_conv = 0;
				for (String url : url_diff) {
					Double diff = Double.parseDouble(url.split(seperator)[1]);
					if (diff <= threshold) {
						num_url_conv += 1;
					}
				}
				System.out.println("num conv: "+num_url_conv+", min_cov: "+min_url_conv+", ratio: "+ (num_url_conv*100 / (updateDiff.count())));
				ctx.getKVS().delete(updateDiff.getName());
				if (num_url_conv >= min_url_conv) {
					break;
				}
			}
//			ctx.getKVS().delete(updateDiff.getName());
			System.out.println("pagerank: iteration " + iteration + ", maxDiff: " + Double.parseDouble(maxDiff));
			logger.info("pagerank: iteration " + iteration + ", maxDiff: " + Double.parseDouble(maxDiff));
			
			ctx.getKVS().delete("pageranks");
			ctx.getKVS().persist("pageranks");
			FlamePairRDD resPairRDD = stablePairRDD.flatMapToPair(pair -> {
				String url = pair._1();
				String valLst = pair._2();
				String rankVal = valLst.split(",")[0];
				KVSClient kvs = new KVSClient(hostAndPort);
				kvs.put("pageranks", url, "rank", rankVal);
				Iterable<FlamePair> iter = new ArrayList<FlamePair>();

				return iter;
			});
			ctx.getKVS().delete(resPairRDD.getName());
			System.out.println("pagerank: iteration " + iteration + ", size: " + ctx.getKVS().count("pageranks"));

			if (Double.parseDouble(maxDiff) < threshold || iteration >= 50) {
				break;
			}
		}

		System.out.println("pagerank: iteration done.");
		logger.info("pagerank: iteration done.");
//		ctx.getKVS().persist("pageranks");
//		FlamePairRDD resPairRDD = stablePairRDD.flatMapToPair(pair -> {
//			String url = pair._1();
//			String valLst = pair._2();
//			String rankVal = valLst.split(",")[0];
//			KVSClient kvs = new KVSClient(hostAndPort);
//			kvs.put("pageranks", url, "rank", rankVal);
//
//			Iterable<FlamePair> iter = new ArrayList<FlamePair>();
//
//			return iter;
//		});
//		ctx.getKVS().delete(resPairRDD.getName());
		ctx.getKVS().delete(stablePairRDD.getName());
		// ctx.getKVS().delete(stablePairRDD.getName());
		System.out.println("pagerank: all done, size: " + ctx.getKVS().count("pageranks"));
		logger.info("pagerank: all done, size: " + ctx.getKVS().count("pageranks"));

		ctx.output("OK");

	}

	/*
	 * Helper functions
	 */
	private static LinkedList<String> extractURL(String pageContent) {
		LinkedList<String> urlLst = new LinkedList<String>();
		// Split the input string into an array of individual tags
		String[] inputSequence = pageContent.split("(?i)(?=<)");

		// Loop through the input sequence
		for (String inputTag : inputSequence) {
			// Ignore closing tags
			if (inputTag.startsWith("</")) {
				continue;
			}
			// Find the start and end indices of the anchor tag
			int start = inputTag.indexOf("<a ");
			if (start == -1) {
				inputTag.indexOf("<A ");
			}
			int end = inputTag.indexOf(">", start);

			// Extract anchor tag attributes if the tag exists, case-insensitive
			if (start != -1 && end != -1) {
				String tag = inputTag.substring(start, end + 1);
				String lowerCaseTag = tag.toLowerCase();

				// Extract the URL attribute from the tag
				int urlStart = lowerCaseTag.indexOf("href=\"");
				if (urlStart != -1) {
					urlStart += 6;
					int urlEnd = lowerCaseTag.indexOf("\"", urlStart);
					String url = tag.substring(urlStart, urlEnd);
					urlLst.add(url);
				}
			}
		}
		return urlLst;
	}

	public static LinkedList<String> normalizeURL(LinkedList<String> urlLst, String parentURL) {
		HashSet<String> resLst = new HashSet<String>();

		// parse order: [0]: protocol, [1]: host, [2]: port, [3]: path ->
		// protocol://host:port/path
		String[] parentRes = parseURL(parentURL);

		for (String url : urlLst) {
			// find item after '#', if empty, discard it
			int pnd = url.indexOf("#");
			if (pnd != -1) {
				url = url.substring(0, pnd);
			}
			if (url.length() == 0) {
				continue;
			}

			String[] urlRes = parseURL(url);
			String res = "";

			if (urlRes[0] == null && urlRes[1] == null && urlRes[2] == null) {
				if (urlRes[3] == null) {
					continue;
				}
				res += parentRes[0] + "://" + parentRes[1] + ":" + parentRes[2];

				if (Character.toString(urlRes[3].charAt(0)).equals(".")) {
					String urlPath = urlRes[3];
					int lastDot = urlPath.lastIndexOf("./");
					String urlDir = urlPath.substring(lastDot + 1);

					String parentDir = parentRes[3];
					if (urlPath.startsWith("../")) {
						parentDir = parentDir.substring(0, parentDir.lastIndexOf("/"));
						boolean breakFlag = false;
						while (urlPath.contains("../")) { // iteratively go up the layer

							int pIdx = parentDir.lastIndexOf("/");
							int uIdx = urlPath.lastIndexOf("..");

							if (pIdx == -1 || uIdx == -1) {
								breakFlag = true;
								break;
							} // if url wants to go above max layer of parent, filter
							parentDir = parentDir.substring(0, pIdx);
							urlPath = urlPath.substring(urlPath.indexOf("..") + 2);
						}
						if (breakFlag) {
							continue;
						}
					} else {
						continue; // weird url that starts with . but not ../, filter
					}
					res += parentDir + urlDir;
				} else if (Character.toString(urlRes[3].charAt(0)).equals("/")) {
					res += urlRes[3];
				} else {
					// relative path
					int lastSlash = parentRes[3].lastIndexOf("/"); // should all have a / at the end? No.
					if (lastSlash != -1) {
						res += parentRes[3].substring(0, lastSlash) + "/" + urlRes[3]; // e.g. parentURL = /bar/xyz.html
																						// -> /bar
					} else {
						res += parentRes[3] + "/" + urlRes[3];
					}
				}
			} else {
				if (urlRes[0] == null) {
					res += "http";
				} else {
					res += urlRes[0];
				}

				if (urlRes[1] == null) {
					continue; // has port but not host, filter
				} else {
					res += "://" + urlRes[1];
					if (urlRes[2] == null) { // has host but not port
						if (urlRes[0] == null) {
							res += ":80";
						} else if (urlRes[0].equals("https")) {
							res += ":443";
						} else if (urlRes[0].equals("http")) {
							res += ":80";
						} else {
							continue; // filter if protocol is invalid
						}
					} else { // has protocol, host and port
						res += ":" + urlRes[2];
					}
				}

				if (urlRes[3] == null) {
					res += "/";
				} else {
					res += urlRes[3];
				}
			}
			// suffix and prefix filter
			if (res.toLowerCase().endsWith(".jpg") || res.toLowerCase().endsWith(".jpeg")
					|| res.toLowerCase().endsWith(".gif") || res.toLowerCase().endsWith(".png")
					|| res.toLowerCase().endsWith(".txt") || res.toLowerCase().endsWith(".pdf")) {
				continue;
			}
			if (!res.startsWith("http") && !res.startsWith("https")) {
				continue;
			}

			// Char to UTF-8 encoding
			try {
				res = encodeURL(res);
				URI encodingCheck = new URL(res).toURI();
			} catch (Exception e) {
				System.out.println("Invalid URL encoding!!");
				continue;
			} // filter if URL contains invalid characters

			resLst.add(res);
		}
		return new LinkedList<String>(resLst);
	}

	public static String encodeURL(String url) {
		url = url.strip();
		try {
			if (url.startsWith("http") || url.startsWith("https")) {
				URL resURL = new URL(URLDecoder.decode(url, StandardCharsets.UTF_8));
				URI resURI = new URI(resURL.getProtocol(), resURL.getUserInfo(), resURL.getHost(), resURL.getPort(),
						resURL.getPath(), resURL.getQuery(), resURL.getRef());
				url = resURI.toASCIIString();
			} else {
				// for BOT path encoding
				url = URLDecoder.decode(url, StandardCharsets.UTF_8);
				URI resURI = new URI(url);
				url = resURI.toASCIIString();
			}
		} catch (Exception e) {
			return null;
		}
		url = url.replaceAll("\\.\\.", "%2e%2e").replaceAll(",", "%2C").replaceAll(" ", "%20");
		return url;
	}

	private static String[] parseURL(String url) {
		String result[] = new String[4];
		int slashslash = url.indexOf("//");
		if (slashslash > 0) {
			result[0] = url.substring(0, slashslash - 1);
			int nextslash = url.indexOf('/', slashslash + 2);
			if (nextslash >= 0) {
				result[1] = url.substring(slashslash + 2, nextslash);
				result[3] = url.substring(nextslash);
			} else {
				result[1] = url.substring(slashslash + 2);
				result[3] = "/";
			}
			int colonPos = result[1].indexOf(':');
			if (colonPos > 0) {
				result[2] = result[1].substring(colonPos + 1);
				result[1] = result[1].substring(0, colonPos);
			}
		} else {
			result[3] = url;
		}

		return result;
	}
}
