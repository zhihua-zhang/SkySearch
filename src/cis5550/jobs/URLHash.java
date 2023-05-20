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

public class URLHash {
	private static final String separator = ",";

	public static void run(FlameContext ctx, String[] urlArg) throws Exception {
		try {
			FlameRDD rdd = ctx.fromTable("crawl", row -> Hasher.hash(row.get("url")) + separator + row.get("url"));
			writeHashUrl(ctx.getKVS(), rdd);
            ctx.getKVS().delete(rdd.getName());
		} catch (Exception e) {}
	}
    
    public static void writeHashUrl(KVSClient client, FlameRDD rdd) {
		try {
            File f = new File("hash2url.txt");
            BufferedWriter bw = new BufferedWriter(new FileWriter(f, false));
			Iterator<Row> iter = client.scan(rdd.getName());
			while (iter.hasNext()) {
				Row row = iter.next();
                try {
                    bw.write(row.get("value"));
                    bw.newLine();
                } catch (Exception e) {}
			}
            bw.close();
		} catch (Exception e) { }
	}

}