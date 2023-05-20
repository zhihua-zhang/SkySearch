package cis5550.generic;
import java.io.IOException;
import java.net.MalformedURLException;

import cis5550.tools.HTTP;
import cis5550.tools.HTTP.Response;

public class Worker {
	
	// create a thread that makes the periodic /ping req
	public static void startPingThread(String masterInfo, String id, String workerPort) {
		(new Thread("Ping") {
			@Override
			public void run() {
				while (true) {
					try {
						String urlArg = "http://" + masterInfo + "/ping?id=" + id + "&port=" + workerPort;
						Response r = HTTP.doRequest("GET", urlArg, null);
						
						if (r.statusCode() != 200) {System.out.println("Fail to ping worker "+id+"!!");}
						System.out.println("Pinged worker: "+id);
						
						Thread.sleep(5000);
						
					} catch (MalformedURLException e1) {e1.printStackTrace();
					} catch (IOException e2) {e2.printStackTrace();
					} catch (InterruptedException e3) {e3.printStackTrace();}

				}
			}
		}).start();
		
	}

}
