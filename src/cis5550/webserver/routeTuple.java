package cis5550.webserver;

public class routeTuple {
	String method;
    String path;
    Route L;
    routeTuple (String method, String path, Route L) {
    	this.method = method;
        this.path = path;
        this.L = L;
    }
}