package cis5550.jobs;

import java.util.*;
import cis5550.flame.*;
import cis5550.tools.*;
import cis5550.kvs.KVSClient;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.util.regex.Matcher;

public class TitleExtract {
	public static void run(FlameContext ctx, String[] urlArg) throws Exception {
		
		ctx.getKVS().persist("titles");
		String hostAndPort = ctx.getKVS().getMaster();
		
		FlameRDD rdd = ctx.fromTable("crawl", row -> {
			
			KVSClient kvs = new KVSClient(hostAndPort);
			
			String url = row.get("url");
			String page = row.get("page");
			String title = "";
			
			if (page != null) {
				// find the title
				Pattern p = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL); 
				Matcher m = p.matcher(page);
				if (m.find()) {
				    title = m.group(1);
				}
				
				// if can't find <title>, take top 100 char from the first <p>...</p>
				if (title.equals("")) {
					Pattern pattern = Pattern.compile("<p>(.*?)</p >");
					Matcher matcher = pattern.matcher(page);
				    if (matcher.find()) {
				    	title = matcher.group(1);
				    	title = title.replaceAll("\\<.*?\\>", "");
						if (!title.equals("") && title.length() > 100) {title = title.substring(0,100);}
				      }
				}
			} 
			
			// if no match of title and <p>, return the original url
			if (title.equals("") && url != null) title = url; 
			
			title = title.replaceAll("\n", " ").replaceAll("\\s+", " ").trim();

			if (title.equals("")) {title = "No title extracted."; System.out.println("Empty after replacement.");}
			
			try {
				kvs.put("titles", Hasher.hash(url), "title", title);
			} catch (Exception e) {System.out.println("Error occur during KVS put!!");}
			
			return "dummy";
		});
		
		String tabName = rdd.getName();
		ctx.getKVS().delete(tabName);
		
		ctx.output("OK");
		
	}
}
