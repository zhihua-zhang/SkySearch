package cis5550.frontend;

import static cis5550.webserver.Server.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;


public class LoadBalancer extends cis5550.generic.Master {
	public static int portNo;
	public static int seq = 1;
	
	public static void main(String[] args) {
		portNo = Integer.parseInt(args[0]);
		port(portNo);
		registerRoutes();
		get("/", (req,res) -> {
			List<String> workers = getWorkers();
	        int assignWorker = seq % workers.size();
	        seq++;
			
			res.header("Location","http://"+workers.get(assignWorker)+"/index.html"); 
			res.status(303,"See Other");
			return null;
			
		});
		
		get("/workerTable", (req,res) -> {res.type("text/html"); return "<html><h1>Worker Table</h1></html>\n" + workerTable();});

	}
	
	
}
