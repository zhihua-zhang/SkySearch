package cis5550.jobs;

import static cis5550.webserver.Server.*;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Vector;

import cis5550.flame.*;
import cis5550.tools.Stemmer;
import cis5550.tools.*;
import cis5550.kvs.*;

public class Debugger {
	private static final Logger logger = Logger.getLogger(Debugger.class);

    public static class pageTuple {
        String url;
        double pagerank;
        pageTuple(String url, double pagerank) {
            this.url = url;
            this.pagerank = pagerank;
        }
    }
    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Debuger: Wrong number of arguments: <kvsMasterIP:port>");
            System.exit(1);
        }
        // port(9999);
        String masterArg = args[0];
        KVSClient client = new KVSClient(masterArg);
        try {
            logger.debug("regex debugging:");
            logger.debug("=" + "沃顿中国".replaceAll("\\p{Punct}", "").replaceAll("[^\\p{L}^\\d]", "").matches("[a-zA-Z0-9\\p{Punct}\\s]*"));
            logger.debug("=" + "Кошница | IKEA България".replaceAll("\\p{Punct}", "").replaceAll("[^\\p{L}^\\d]", "").matches("[a-zA-Z0-9\\p{Punct}\\s]*"));
            logger.debug("=" + "?asd^^&".replaceAll("\\p{Punct}", "").replaceAll("[^\\p{L}^\\d]", "").matches("[a-zA-Z0-9\\p{Punct}\\s]*"));
            logger.debug("=" + "/?".split("\\?", 2)[0]);
            // Row row = client.getRow("titles", "tcneuoaimccqrksaugcadqxguajgxowqnavqnsjs");
            // logger.debug("row is null: " + (row == null));
            // logger.debug("row: " + row.key());
            // for (String col : row.columns()) {
            //     logger.debug("col: " + col + ", " + "value: " + row.get(col));
            // }
            // Iterator<Row> iter = client.scan("pageranks");
            // Vector<pageTuple> pgs = new Vector<>();
            // while (iter.hasNext()) {
            //     Row row = iter.next();
            //     pgs.add(new pageTuple(row.key(), Double.valueOf(row.get("rank"))));
            // }
            // pgs.sort(new Comparator<pageTuple>() {
            //     @Override
            //     public int compare(pageTuple x1, pageTuple x2) {
            //         return x1.pagerank - x2.pagerank >= 0 ? -1 : 1; // high score put at front
            //     }
            // });
            // for (int i=0; i<20 ;i++) {
            //     pageTuple pg = pgs.get(i);
            //     logger.debug("top " + i + " page: url=" + pg.url + ", pagerank=" + pg.pagerank);
            // }
        } catch (Exception e) {
            logger.error("debugging err: " + e.getMessage());
        }
    }
}
