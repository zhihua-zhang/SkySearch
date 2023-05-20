package cis5550.kvs;
import static cis5550.webserver.Server.*;

public class Master extends cis5550.generic.Master{
	public static int portNo;

	public static void main(String[] args) {
		portNo = Integer.parseInt(args[0]);
		port(portNo);
		registerRoutes();
		get("/", (req,res) -> {res.type("text/html"); return "<html><h1>Worker Table</h1></html>\n" + workerTable();});
	}

}
