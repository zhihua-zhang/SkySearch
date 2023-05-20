package cis5550.generic;
import static cis5550.webserver.Server.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Master {
	
	public static ConcurrentHashMap <String, ArrayList<String>> workers = new ConcurrentHashMap <>(); // {id: [ip, port, last ping time]}
	
	// return the current list of workers as ip:port strings
	public static List<String> getWorkers(){
		List<String> currentWorkers = new LinkedList<String>();
		
		for (String id: workers.keySet()) {
			if ( System.currentTimeMillis() - Long.parseLong(workers.get(id).get(2)) <= 15*1000) {
				currentWorkers.add(workers.get(id).get(0) + ":" + workers.get(id).get(1));
			} else {
				workers.remove(id);
			}
		}
		return currentWorkers;
	}
	
	// return the current list of workers as id,ip:port strings
		public static List<String> getWorkersWithID(){
			List<String> currentWorkers = new LinkedList<String>();
			
			for (String id: workers.keySet()) {
				if ( System.currentTimeMillis() - Long.parseLong(workers.get(id).get(2)) <= 15*1000) {
					currentWorkers.add(id + "," + workers.get(id).get(0) + ":" + workers.get(id).get(1));
				} else {
					workers.remove(id);
				}
			}
			System.out.println("Get workers list: " + currentWorkers);
			return currentWorkers;
		}
	 
	// return the HTML table with the list of workers
	public static String workerTable() {
		String table = "<html><table>\n<tr><th>ID</th><th>IP</th><th>Port</th><th>HyperLink</th></tr>\n";
	    for (String id: workers.keySet()) {
	    	if ( System.currentTimeMillis() - Long.parseLong(workers.get(id).get(2)) <= 15*1000) {
		    	String ip = workers.get(id).get(0);
		    	String port = workers.get(id).get(1);
		    	String url = "http://" + ip + ":" + port + "/";
		        table += "<tr><td>" + id + "</td><td>" + ip + "</td><td>" + port + "</td><td>" + "<a href="+url+">"+ url + "</a>"+ "</td></tr>\n";
	    	}
	    }
	    table += "</table></html>";
		return table; 
	}
	
	// create routes for the /ping and /workers routes (but not the /route)
	public static void registerRoutes() {
		// register a new worker with GET /ping?id=x&port=y
		get("/ping", (req,res) -> {
			if (req.queryParams("id") == null || req.queryParams("port") == null) {res.status(400, "Bad request"); return "400 Bad request";};
			workers.put(req.queryParams("id"), new ArrayList<String>(Arrays.asList(req.ip(),req.queryParams("port"),""+System.currentTimeMillis())));
			return "OK";
		});
		// return k+1 lines separated by LFs; 1st line contains k, each line has a worker (id, ip:port)
		get("/workers", (req, res) -> {List<String> workerLst= getWorkersWithID(); return workerLst.size()+ "\n" + String.join("\n", workerLst);});
		
	}

}
