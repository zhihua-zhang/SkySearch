package cis5550.frontend;

import static cis5550.webserver.Server.*;
import cis5550.kvs.*;

import java.io.*;
import java.net.*;
import java.util.*;
import com.google.gson.*;
import javax.net.ssl.HttpsURLConnection;

public class Server extends cis5550.generic.Worker{
	
	static String host = "https://api.bing.microsoft.com";
	static String spellCheckPath = "/v7.0/spellcheck";
	static String trendingPath = "/v7.0/news/trendingtopics";
	static String key = "61801dad0db14c839172b0c96aa72b16";
	static String mkt = "en-US";
	static String mode = "proof";
	static String trendingTopics = "";
    
    static class SearchResults{
        HashMap<String, String> relevantHeaders;
        String jsonResponse;
        SearchResults(HashMap<String, String> headers, String json) {
            relevantHeaders = headers;
            jsonResponse = json;
        }
    }
    
    public static String check (String text) throws Exception {
        String params = "?mkt=" + mkt + "&mode=" + mode;
        URL url = new URL(host + spellCheckPath + params);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", key);
        connection.setDoOutput(true);
        
        DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
        wr.writeBytes("text=" + text);
        wr.flush();
        wr.close();
        
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String line;
		String result = "";
		while ((line = in.readLine()) != null) {
			result += prettify(line);
		}
		in.close();
		
		return result;
    }
    
    public static String trending() throws Exception {
        String params = "?mkt=" + mkt;
        URL url = new URL(host + trendingPath + params);
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", key);
        // receive JSON body
        InputStream stream = connection.getInputStream();
        String response = new Scanner(stream).useDelimiter("\\A").next();
        // construct result object for return
        SearchResults results = new SearchResults(new HashMap<String, String>(), response);
		
        // extract Bing-related HTTP headers
        Map<String, List<String>> headers = connection.getHeaderFields();
        for (String header : headers.keySet()) {
            if (header == null) continue;      // may have null key
            if (header.startsWith("BingAPIs-") || header.startsWith("X-MSEdge-")) {
                results.relevantHeaders.put(header, headers.get(header).get(0));
            }
        }
        stream.close();
        
		return prettify(results.jsonResponse);
    }
    
    public static String prettify(String json_text) {
        JsonParser parser = new JsonParser();
        JsonElement json = parser.parse(json_text);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(json);
    }
    
	public static void main(String[] args) {
		if (args.length != 3) {
	    	System.err.println("Syntax: Worker <port> <balancerIP:port> <kvMasterIP:port>");
	    	System.exit(1);
	    }

        int workerPort = Integer.valueOf(args[0]);
        port(workerPort);
	  	
        String remoteHost = args[1];

        String workerID="";
        Random rand = new Random();
        for (int i=0; i<5; i++) {
            workerID += "abcdefghijklmnopqrstuvwxyz".charAt(rand.nextInt(25));
        }
        startPingThread(remoteHost, workerID, workerPort+"");
        
        String kvsMaster = args[2];
        KVSClient kvs = new KVSClient(kvsMaster);

	    staticFiles.location("./public");
	    Gson gson = new Gson();
	    

//	    get("/suggest", (req, res) -> {
//	        res.header("Access-Control-Allow-Origin", "*");
//	    	res.header("Content-Type","application/xml");
//	    	
//	    	if(trendingTopics.equals("")) trendingTopics = trending();
//	        return trendingTopics;
//	    });
	    
	    get("/check/:term", (req, res) -> {
	        res.header("Access-Control-Allow-Origin", "*");
	    	res.header("Content-Type","application/json");
	        System.out.println("Requested check term: "+req.params("term"));
	    	return check(req.params("term"));
	    });
	    
	    get("/cached/:id", (req, res) -> {
	    	String id = req.params("id");
	        res.header("Access-Control-Allow-Origin", "*");
			res.type("text/html");

            res.write(kvs.get("crawl",id,"page"));
            return null;
	    });
	    
	    
//	     get("/search/:term", (req, res) -> {
//	     	res.header("Content-Type","application/json");
//	         res.header("Access-Control-Allow-Origin", "*");
//	         System.out.println("Requested search term: "+req.params("term"));
//	         String urls[] = { "https://en.wikipedia.org/wiki/Gaussian_integer", 
//	  	           "https://en.wikipedia.org/wiki/Number_theory", "https://en.wikipedia.org/wiki/Pure_mathematics","https://en.wikipedia.org/wiki/Bacteria","https://en.wikipedia.org/wiki/Wool","https://en.wikipedia.org/wiki/Sheep","https://en.wikipedia.org/wiki/Dialect","https://en.wikipedia.org/wiki/Social_class","https://en.wikipedia.org/wiki/History","https://en.wikipedia.org/wiki/Historiography","https://en.wikipedia.org/wiki/Egypt","https://en.wikipedia.org/wiki/Latin" };
//	        
//	         ArrayList<ResultItem> results = new ArrayList<>();
//	        
//	         for (String url : urls) {
//	         	ResultItem obj = new ResultItem(url, 1, 10,10,"11");
//	         	results.add(obj);
//	         }
//	        
//	         String json = gson.toJson(results); 
//
//	         return json;
//	     });
	    
	}
}
