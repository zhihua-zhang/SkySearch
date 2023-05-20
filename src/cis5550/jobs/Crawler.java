package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;
import java.io.*;

import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;

public class Crawler {
	
	public static final int MAX_PAGE_SIZE = 3; // in megabytes
	public static final int MAX_URL_QUEUE_LENGTH = 4000;
	public static final int MAX_URL_PER_SITE = 5000;
	public static final int MAX_URL_PER_HOST = 10000;
	public static final double PRIORITIZE_RATIO = 0.6;
	
	public static void run(FlameContext ctx, String[] urlArg) throws Exception{
		if (urlArg.length != 1) {
			ctx.output("Provide only one argument: seedURL");
			System.exit(1);
		} else {ctx.output("OK");}
		
		// create a persistent crawl table
		if (ctx.getKVS().count("crawl") == 0) {System.out.println("Create a new crawl table..."); ctx.getKVS().persist("crawl");}
		System.out.println("Existing num of crawl records: "+ctx.getKVS().count("crawl"));
		
		// create a persistent hostCnt table
		if (ctx.getKVS().count("hostCnt") == 0) {System.out.println("Create a new hostCnt table..."); ctx.getKVS().persist("hostCnt");}
		System.out.println("Existing num of hostCnt records: "+ctx.getKVS().count("hostCnt"));
		
		// create a persistent hashedContent table
		if (ctx.getKVS().count("hashedContent") == 0) {System.out.println("Create a new hashedContent table..."); ctx.getKVS().persist("hashedContent");}
		System.out.println("Existing num of hashedContent records: "+ctx.getKVS().count("hashedContent"));
		
		// create a persistent blacklist table
		LinkedList<Pattern> blacklistURL = new LinkedList<Pattern>();
		if (ctx.getKVS().count("blacklist") == 0) {
			System.out.println("Create a new blacklist table...");
			ctx.getKVS().persist("blacklist");
		} else {
			Iterator<Row> rowLst = ctx.getKVS().scan("blacklist");
			while (rowLst.hasNext()) {
				String val = rowLst.next().get("pattern");
				if (val != null) {
//					String newVal = val.replaceAll("\\*", ".*?");
					Pattern pattern = Pattern.compile(val);
					blacklistURL.add(pattern);
				}
			}
		}
		System.out.println("Existing num of blacklist records: "+ctx.getKVS().count("blacklist"));
		
		
		LinkedList<String> seedURL = new LinkedList<String>();
		seedURL.add(urlArg[0]);
		seedURL = normalizeURL(seedURL, "");
		System.out.println("Seed url after normalization: " + seedURL.get(0));
		
		FlameRDD urlQueue = ctx.parallelize(seedURL);
		String hostAndPort = ctx.getKVS().getMaster();
		
		while (urlQueue.count() > 0) {
			// the parentURL passed in should have all elements required (protocol, host, port, path)
			System.out.println("Start flatMap...");
			Long time1 = System.currentTimeMillis();
			try {
				urlQueue = urlQueue.flatMap(parentURL -> {		
					KVSClient kvs = new KVSClient(hostAndPort);
					LinkedList<String> crawledURL = new LinkedList<String>();
					System.out.println("parentURL: "+parentURL);
					
					// URL-seen test
					System.out.println("URL seem test...");
					String R = Hasher.hash(parentURL);
					if (kvs.existsRow("crawl", R)) {System.out.println("Parent URL already existed!!"); return crawledURL;}
					Row currRow = new Row(R);
					currRow.put("url", parentURL);
					
					String[] parsedParentURL = parseURL(parentURL);
					String host = parsedParentURL[1];
//					String hostR = Hasher.hash(host);
					String hostR = host;
					System.out.println("host name: "+parsedParentURL[1]);
					
					// robot exclusion protocol
					System.out.println("Check robot exclusion protocol...");
					if (kvs.get("hosts", hostR, "bot") == null) {
						System.out.println("Downloading BOT file...");
						try {
							Long timeR1 = System.currentTimeMillis();
							String botURL = parsedParentURL[0] + "://" + parsedParentURL[1] + ":" + parsedParentURL[2] + "/robots.txt";
							HttpURLConnection botConnection = (HttpURLConnection) new URL(botURL).openConnection();
							botConnection.setRequestMethod("GET");
							botConnection.setRequestProperty("User-Agent", "cis5550-crawler");
							botConnection.setConnectTimeout(5*1000);
							botConnection.setReadTimeout(5*1000);
							botConnection.connect();
							Long timeR2 = System.currentTimeMillis();
							System.out.println("Time for BOT download: "+ (timeR2-timeR1));
							
							if (botConnection.getResponseCode() == 200) {
								String cleanedBOT = parseBotFile(botConnection.getInputStream().readAllBytes());
								kvs.put("hosts", hostR, "bot", cleanedBOT);
							} else {
								kvs.put("hosts", hostR, "bot", "N/A");
							}
							Long timeR3 = System.currentTimeMillis();
							System.out.println("Time for BOT parsing: "+ (timeR3-timeR2));
						} catch (Exception e) { // if unable to download bot file, skip this url
							System.out.println("Unable to download or parse BOT file! Skip this url.");
							addToBlacklist(parentURL, blacklistURL, hostAndPort, "Unable to download or parse BOT file.");
							return crawledURL;
						}
						
					}
					
					byte[] botContent = kvs.get("hosts", hostR, "bot");
					if (botContent == null) {
						System.out.println("Transient error for KVS get operation occur!!");
						crawledURL.add(parentURL);
						return crawledURL;
					}
					
					HashMap<String, String> botMap = checkBotFile(botContent, parsedParentURL[3]);
					if (botMap.get("disallow") != null) {System.out.println("Disallowed url!!"); return crawledURL;}

					// check max url per host
					if (kvs.getRow("hostCnt", hostR) == null) {kvs.put("hostCnt", hostR, "URLCnt", "0");}
					Long URLCnt = Long.parseLong(kvs.getRow("hostCnt", hostR).get("URLCnt"));
					if (URLCnt > MAX_URL_PER_HOST) {
						 System.out.println("Reached max # of url allowed for this host: "+hostR);
						 addToBlacklist(".*?://"+host+":.*?", blacklistURL, hostAndPort, "Reached max # of url allowed for this host.");
						 return crawledURL;
					}
					
					// add rate limit
					System.out.println("Check rate limit...");
					String wait = botMap.get("crawl-delay");
					try {
						double waitSec = (wait == null)? 0.5 : Float.parseFloat(wait);
						String lastAccessedTime = kvs.getRow("hosts", hostR).get("lastAccessedTime");
						if ((lastAccessedTime != null) && (System.currentTimeMillis() - Long.parseLong(lastAccessedTime) <= waitSec*1000)) {
							System.out.println("Access frequency for this host is too high!! WaitSec: "+waitSec);
							crawledURL.add(parentURL);
							return crawledURL;
						}
					} catch (NumberFormatException e) {
						System.out.println("Crawl-delay is not formatted correctly in BOT: "+wait); 
						addToBlacklist(parentURL, blacklistURL, hostAndPort, "Crawl-delay is not formatted correctly in BOT.");
						return crawledURL;
					}
					
					try {
						// HEAD req before GET
						System.out.println("Send HEAD request...");
						HttpURLConnection headConnection = (HttpURLConnection) new URL(parentURL).openConnection();
						headConnection.setRequestMethod("HEAD");
						headConnection.setRequestProperty("User-Agent", "cis5550-crawler");
						headConnection.setInstanceFollowRedirects(false); 
						headConnection.setConnectTimeout(5*1000);
					    headConnection.setReadTimeout(5*1000);
						headConnection.connect();
						kvs.put("hosts", hostR, "lastAccessedTime", System.currentTimeMillis()+"");
						
						int headResponseCode = headConnection.getResponseCode();
						String contentType = headConnection.getHeaderField("Content-Type");
						String contentLength = headConnection.getHeaderField("Content-Length");
						
						if (contentType != null) {currRow.put("contentType", contentType);}
						if (contentLength != null) {
							currRow.put("length", contentLength);
							// gigantic file check
							if (Long.parseLong(contentLength) > MAX_PAGE_SIZE*1024*1024) {
								System.out.println("Page too large to download! Skip this URL.");
								addToBlacklist(parentURL, blacklistURL, hostAndPort, "Page too large to download.");
								return crawledURL;
							}
						}
						
						if (headResponseCode != 200)  {
							System.out.println("Got response code other than 200...");
							 currRow.put("responseCode", headResponseCode+"");
							 if (headResponseCode == 301 || headResponseCode == 302 || headResponseCode == 303 || headResponseCode == 307 || headResponseCode == 308) {
								 System.out.println("Response code is " + headResponseCode + ": redirecting...");
								 String redirectLoc = headConnection.getHeaderField("Location");
								 LinkedList<String> redirectURL = new LinkedList<String>();
								 redirectURL.add(redirectLoc);
								 redirectURL = normalizeURL(redirectURL, parentURL);
								 if (blacklistURL.size() > 0) {redirectURL = blacklistFilter(redirectURL, blacklistURL);}
								 if (redirectURL.size() > 0) {crawledURL.add(redirectURL.get(0));}
								 addToBlacklist(parentURL, blacklistURL, hostAndPort, "Redirected URL.");
							 } else {
								 System.out.println("Code other than 200 and redirect: " + headResponseCode);
								 addToBlacklist(parentURL, blacklistURL, hostAndPort, "Code other than 200 and redirect.");
							 }
							 return crawledURL;
						} else {
							System.out.println("Response code is 200, sending GET request...");
							HttpURLConnection getConnection = (HttpURLConnection) new URL(parentURL).openConnection();
							getConnection.setRequestMethod("GET");
							getConnection.setRequestProperty("User-Agent", "cis5550-crawler");
							getConnection.connect();
							kvs.put("hosts", hostR, "lastAccessedTime", System.currentTimeMillis()+"");

					        int getResponseCode = getConnection.getResponseCode();
					        currRow.put("responseCode", getResponseCode+"");
					        
					        if (contentType != null && contentType.contains("text/html")) {
					        	byte[] pageContent = getConnection.getInputStream().readAllBytes();
					        	String filteredPage = filterPage(pageContent);
					        	
					        	if (filteredPage == null) {System.out.println("Page is null! Skip this url."); addToBlacklist(parentURL, blacklistURL, hostAndPort, "Page is null!"); return crawledURL;}
					        	
					        	// check if the page contains non-english contents
					        	boolean englishPage = languageCheck(filteredPage);
					        	if (!englishPage) {
					        		System.out.println("Page content not in English: " + parentURL); 
					        		addToBlacklist(parentURL, blacklistURL, hostAndPort, "Page content not in English.");
					        		return crawledURL;
					        	}
					        	
					        	// content-seen test via fingerprinting (64 digits)
					        	String hashedpage = SHA256Hash(filteredPage);
					        	if (kvs.existsRow("hashedContent", hashedpage)) {
					        		System.out.println("Page already exsisted: " + parentURL);
					        		return crawledURL;
					        	} else {
					        		currRow.put("page", filteredPage);
					        		kvs.put("hashedContent", hashedpage, "url", Hasher.hash(parentURL));
					        	}
					        	
					        	LinkedList<String> extractedURL = (pageContent != null)? extractURL(filteredPage) : new LinkedList<String>();
					        	LinkedList<String> normalizedURL = normalizeURL(extractedURL, parentURL);
					        	if (blacklistURL.size() > 0) {normalizedURL = blacklistFilter(normalizedURL, blacklistURL);}
					        	crawledURL.addAll(normalizedURL);
					        	kvs.put("hostCnt", hostR, "URLCnt",  (URLCnt + 1) +"");
					        }
						}
					} catch  (SocketTimeoutException e) {
						System.out.println("Connection socket time out! Skip this url.");
						addToBlacklist(parentURL, blacklistURL, hostAndPort, "Connection socket time out.");
						return crawledURL;
					} catch (Exception e) {
						System.out.println("Other exception occurs during HEAD/GET! Skip this url.");
						addToBlacklist(parentURL, blacklistURL, hostAndPort, "Other exception occurs during HEAD/GET.");
						return crawledURL;
					}
					
				    kvs.putRow("crawl", currRow);
				    System.out.println("Put row: " + currRow.key()); 
				    System.out.println("# Normalized extracted url: " + crawledURL.size());
				    return crawledURL;
				    
				});
				
				Long time11 = System.currentTimeMillis();
				System.out.println("Before prioritization #urlqueue: " + urlQueue.count());
				if (urlQueue.count() > 2) prioritizeURL(hostAndPort, urlQueue);
				System.out.println("After prioritization #urlqueue: " + urlQueue.count());
				Long time12 = System.currentTimeMillis();
				
				System.out.println("*****************RUNTIME SUMMARY*****************");
				System.out.println("Time for flatMap & prioritization: "+ (time12-time1));
				System.out.println("Time for flatMap: "+ (time11-time1));
				System.out.println("Time for URL prioritization: "+ (time12-time11));
				System.out.println("*************************************************");
				
			} catch (Exception e) {
				System.out.println("Other unexpected error!!");
				e.printStackTrace();
			}
			System.out.println("Thread is put to sleep...");
			Thread.sleep(2*1000);
		}
	}
	
	public static void addToBlacklist(String parentURL, LinkedList<Pattern> blacklistURL, String hostAndPort, String reason) throws Exception {
		try {
			Pattern pattern = Pattern.compile(parentURL);
			blacklistURL.add(pattern);
			KVSClient kvs = new KVSClient(hostAndPort);
			
			Row currRow = new Row(Hasher.hash(parentURL));
			currRow.put("pattern",parentURL);
			currRow.put("reason", reason);
			
			kvs.putRow("blacklist", currRow);	
		} catch (Exception e) {
			System.out.println("Can't compile and put into blacklist pattern: "+parentURL);
			e.printStackTrace();
		}
		
	}
	
	public static void prioritizeURL(String hostAndPort, FlameRDD urlQueue) throws Exception {
		// Only select top n urls: prioritize HTTPS over HTTP; shallow over deep;
		System.out.println("Prioritizing urlQueue...");
		String tabName = urlQueue.getName();
		Vector<String> urls = urlQueue.take(urlQueue.count()+1);
		KVSClient kvs = new KVSClient(hostAndPort);
		kvs.delete(tabName);
		
		Random rand = new Random();
		if (rand.nextDouble() <= PRIORITIZE_RATIO) {
			urls.sort(Comparator.comparing((String url) -> url.startsWith("https:") ? 0 : 1)
		              .thenComparing(url -> url.split("/").length)
		    );
		}
		if (urls.size() > MAX_URL_QUEUE_LENGTH) {urls = new Vector<String>(urls.subList(0, MAX_URL_QUEUE_LENGTH));}
		
		Collections.shuffle(urls);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (String url: urls) {
			String R = Hasher.hash(UUID.randomUUID().toString());
			Row curRow = new Row(R);
			curRow.put("value", url);
		
			out.write(curRow.toByteArray());
			out.write("\n".getBytes());
		}
		out.close();
		kvs.putRows(tabName, out.toByteArray());
		
	}
	
	public static String SHA256Hash(String s) {
		try {
			s = s.replaceAll("[^a-zA-Z]", "").replaceAll("\\s+","").toLowerCase();
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
	        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
	
	        // Convert the hash bytes to a hex string representation
	        StringBuilder hexString = new StringBuilder();
	        for (byte b : hash) {
	            String hex = Integer.toHexString(0xff & b);
	            if (hex.length() == 1) {
	                hexString.append('0');
	            }
	            hexString.append(hex);
	        }
	        return hexString.toString();
		} catch (NoSuchAlgorithmException e) {System.out.println("No such algorithm!!"); return s;}
	}
	
	public static String filterPage(byte[] pageContent) {
		if (pageContent == null) return null;
		String content = new String(pageContent, StandardCharsets.UTF_8);
		// remove java script content
		content = content.replaceAll("<script[^>]*>[\\s\\S]*?</script>", "");
		// remove svg and style tags
		content = content.replaceAll("<svg[^>]*>[\\s\\S]*?</svg>", "").replaceAll("<style[^>]*>[\\s\\S]*?</style>", "");
		return content;
	}
	
	public static boolean languageCheck(String pageContent) {
		// extract <html lang="..." or lang='...'> tag, if not exist assume English
		Pattern pattern = Pattern.compile("<html(?:[^>]+)?\\s+lang=(?:\"|')([^\"']+)(?:\"|')[^>]*>");
		Matcher matcher = pattern.matcher(pageContent);
		boolean flag = true;

		if (matcher.find()) {
			String langValue = matcher.group(1);
			if (!langValue.contains("en")) {flag = false;} // could be en or en-US etc
        }
		return flag;
	}

	public static LinkedList<String> extractURL(String pageContent) {
		LinkedList<String> urlLst = new LinkedList<String>();
		// Split the input string into an array of individual tags
        String[] inputSequence = pageContent.split("(?i)(?=<)");
        int urlCnt = 0;

        // Loop through the input sequence
        for (String inputTag: inputSequence) {
        	if (urlCnt > MAX_URL_PER_SITE) {System.out.println("Reached max # of URL for this URL!!");break;} // Limit maximum URL per site
            if (inputTag.startsWith("</")) {continue;} // Ignore closing tags
            
            // Find the start and end indices of the anchor tag
            int start = inputTag.indexOf("<a ");
            if (start == -1) {inputTag.indexOf("<A ");}
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
                    urlCnt += 1;
                }
            }
       }
       return urlLst;	
	}
	
	public static LinkedList<String> normalizeURL(LinkedList<String> urlLst, String parentURL) {
		HashSet<String> resLst = new HashSet<String>();

		// parse order: [0]: protocol, [1]: host, [2]: port, [3]: path -> protocol://host:port/path
		String[] parentRes = parseURL(parentURL);
		
		for (String url: urlLst) {
			// find item after '#', if empty, discard it
			int pnd = url.indexOf("#");
			if (pnd != -1) {url = url.substring(0, pnd);}
			if (url.length() == 0) {continue;}
			
			String[] urlRes = parseURL(url);
			// remove extra query params
			int and = urlRes[3].indexOf("&");
			if (and != -1 && urlRes[3].contains("?")) {urlRes[3] = urlRes[3].substring(0, and);}
			
			String res = "";
			
			if (urlRes[0] == null && urlRes[1] == null && urlRes[2] == null) {
				if (urlRes[3] == null) {continue;}
				res += parentRes[0]+"://"+parentRes[1]+":"+parentRes[2];

				if (Character.toString(urlRes[3].charAt(0)).equals(".")) {
					String urlPath = urlRes[3];
					int lastDot = urlPath.lastIndexOf("./");
					String urlDir = urlPath.substring(lastDot + 1);
					
					String parentDir = parentRes[3];
					if(urlPath.startsWith("../")) {
						parentDir = parentDir.substring(0,parentDir.lastIndexOf("/"));
						boolean breakFlag = false;
						while(urlPath.contains("../")) { // iteratively go up the layer
							
							int pIdx = parentDir.lastIndexOf("/");
							int uIdx = urlPath.lastIndexOf("..");
							
							if (pIdx == -1 || uIdx == -1) {breakFlag = true; break;} // if url wants to go above max layer of parent, filter
							parentDir = parentDir.substring(0, pIdx);
							urlPath = urlPath.substring(urlPath.indexOf("..")+2);
						}
						if (breakFlag) {continue;} 
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
						res += parentRes[3].substring(0, lastSlash) + "/" + urlRes[3]; // e.g. parentURL = /bar/xyz.html -> /bar
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
				|| res.toLowerCase().endsWith(".txt") || res.toLowerCase().endsWith(".pdf")
				|| res.toLowerCase().endsWith(".js") || res.toLowerCase().endsWith(".css")) {continue;}
			if (!res.startsWith("http") && !res.startsWith("https")) {continue;}
			
			// URL encoding
			try {
				res = encodeURL(res);
				URI encodingCheck = new URL(res).toURI();
			} catch (Exception e) {System.out.println("Invalid URL encoding!!"); continue;} // filter if URL contains invalid characters
			
			resLst.add(res);
		}
		LinkedList<String> finalRes = new LinkedList<String>(resLst);
		return finalRes;
	}
	
	public static String encodeURL(String url) {
		url = url.strip();
		try {
			if (url.startsWith("http") || url.startsWith("https")) {
				URL resURL = new URL(URLDecoder.decode(url, StandardCharsets.UTF_8));
				url = url.replaceAll("\\.\\.", "%2e%2e").replaceAll(",", "%2C").replaceAll(" ", "%20");
				URI resURI = new URI(resURL.getProtocol(), resURL.getUserInfo(), resURL.getHost(), resURL.getPort(), resURL.getPath(), resURL.getQuery(), resURL.getRef());
				url = resURI.toASCIIString();
			} else {
				// for BOT path encoding
				url = URLDecoder.decode(url, StandardCharsets.UTF_8);
				url = url.replaceAll("\\.\\.", "%2e%2e").replaceAll(",", "%2C").replaceAll(" ", "%20");
			}
		} catch (Exception e) {System.out.println("Error in encodeURL!!"+url);e.printStackTrace(); return null;}
		return url;
	}
	
	public static String parseBotFile(byte[] content) {
		if (content == null) return "N/A";
		String cleanedBOT = "";
		
		String text = new String(content, StandardCharsets.UTF_8);
		boolean flag = false;
		
		String agent = (text.toLowerCase().contains("user-agent: cis5550-crawler"))? "cis5550-crawler" : "*";
		String[] lines = text.split("\\r?\\n");
		for (String line: lines) {
			if (line.strip().startsWith("#")) {continue;} // exclude comments
			if (line.equals("")) {continue;}
		    String[] parts = line.split(":", 2);
		    if (parts.length == 2) {
		        String key = parts[0].trim().toLowerCase();
		        String value = parts[1].trim();
		        if (key.equals("user-agent")) {
		        	if (flag) {break;}
		        	flag = (agent.contains(value.toLowerCase()))? true : flag;
		        }
		        if (!flag) {continue;}
	        	if (key.equals("allow") || key.equals("disallow")) {
	        		value = encodeURL(value);
	        		if (value != null) cleanedBOT += key + ":" + value + "\r\n";
	        	} else {
	        		cleanedBOT += line + "\r\n";
	        	}
		    }
		}
		return cleanedBOT;
	}
	
	public static HashMap<String, String> checkBotFile(byte[] content, String host) {
		HashMap<String, String> map = new HashMap<>();
		String text = new String(content, StandardCharsets.UTF_8);
		try {host = java.net.URLDecoder.decode(host, "UTF-8");} catch (UnsupportedEncodingException e) {System.out.println("UTF-8 encoding not supported!!");}
		
		if (!text.equals("N/A")) {
			System.out.println("Checking BOT requirement for current host...");

			String[] lines = text.split("\\r?\\n");
			for (String line: lines) {
			    String[] parts = line.split(":", 2);
			    if (parts.length == 2) {
			        String key = parts[0].trim().toLowerCase();
			        String value = parts[1].trim();

		        	if (key.equals("allow") && !map.containsKey("allow") && !map.containsKey("disallow") && host.startsWith(value)) {
		        		map.put("allow", "true");
		        	}
		        	if (key.equals("disallow") && !map.containsKey("allow") && !map.containsKey("disallow") && host.startsWith(value)) {
		        		map.put("disallow", "true");
		        	}
		        	if (!key.equals("allow") && !key.equals("disallow")) {
		        		value = value.split("#")[0].strip(); // remove in-line comment 
		        		map.put(key, value);
		        	}
			    }
			}
		}
		System.out.println("BOT map: "+map);
		return map;
	}
	
	public static LinkedList<String> blacklistFilter(LinkedList<String> urls, LinkedList<Pattern> blacklist){
		
		HashSet<String> resLst = new HashSet<String>();
		boolean add = true;
		
		for (String url: urls) {
			add = true;
			for (Pattern pat: blacklist) {
				if (pat.matcher(url).matches()) {add = false; break;}
			}
			if (add) {resLst.add(url);}
			else {System.out.println("Blocked by blacklist: "+url);}
		}
		LinkedList<String> finalRes = new LinkedList<String>(resLst);
		return finalRes;
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

