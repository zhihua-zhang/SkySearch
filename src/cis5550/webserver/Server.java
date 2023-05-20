package cis5550.webserver;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ServerSocketFactory;
import javax.net.ssl.* ;
import java.security.* ;

public class Server implements Runnable {
    static Server instance;
    static boolean running = false;
    static Map<String, String> staticFilePath = new HashMap<>();
    static String currentHost="default";
	static int NUM_WORKERS = 100;
    static int port = 80;
    int securePort = -1;

    static BlockingQueue<Socket> queue = new LinkedBlockingQueue<>();
    Map<String, SessionImpl> sessions= new HashMap<>();
    Map<String, ArrayList<routeTuple>> routes;
    
    public Server() {
        port = 8000;
        running = false;
        this.routes = new HashMap<String, ArrayList<routeTuple>>();
        this.routes.put("default", new ArrayList<>());
        this.sessions = new HashMap<String, SessionImpl>();
        this.securePort = -1;
    }

    public static class staticFiles {
        public static void location (String s) {
            checkAndCreateServer();
            staticFilePath.put(currentHost, s);
        }
    }

    public static void get(String p, Route L) {
        checkAndCreateServer();
        instance.addRoute("GET", p, L);
    }

    public static void post(String p, Route L) {
        checkAndCreateServer();
        instance.addRoute("POST", p, L);
    }

    public static void put(String p, Route L) {
        checkAndCreateServer();
        instance.addRoute("PUT", p, L);
    }

    public static void port(int N) {
    	if (instance == null) {
            instance = new Server();
        }
        instance.port = N;
    }

    public static void securePort(int N) {
    	if (instance == null) {
            instance = new Server();
        }
        instance.securePort = N;
    }

    public static void checkAndCreateServer() {
        if (instance == null) {
            instance = new Server();
        }
        if (!running) {
            running = true;
            Thread thread = new Thread(instance);
            thread.start();
        }
    }
    
    public void addRoute(String method, String path, Route L) {
        this.routes.get(currentHost).add(new routeTuple(method, path, L));
    }
    
    
    public void serverLoop(ServerSocket serverSocket) {
        while (true) {
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    queue.put(socket);
                }
            }
            catch (Exception exception) {
                exception.printStackTrace();
                continue;
            }
        }
    }

    
    @Override
    public void run() {
        ServerSocket serverSocket = null;
        ServerSocket serverSocketTLS = null;

		try {
            serverSocket = new ServerSocket(port);
	        if(this.securePort!=-1) {
				String pwd = "secret";
				KeyStore keyStore;
				keyStore = KeyStore.getInstance("JKS");
				keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
				KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
				keyManagerFactory.init(keyStore, pwd.toCharArray());
				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
				ServerSocketFactory factory = sslContext.getServerSocketFactory();
				serverSocketTLS = factory.createServerSocket(securePort);
				
		        final ServerSocket serverSocketSecure = serverSocketTLS;
				new Thread(){
	                @Override
	                public void run() {
	                    serverLoop(serverSocketSecure);
	                }
	            }.start();
	        }
		}catch (Exception exception) {
            exception.printStackTrace();
        }
		running = true;

		for(int i=0;i<NUM_WORKERS;i++) {
            new Thread(){
                @Override
                public void run() {
                	ThreadServer newThread = new ThreadServer();
                	newThread.workerThread();
                }
            }.start();
		}
        if(this.securePort!=-1) {
        }		
		Thread checkSession = new Thread(){
		    public void run() {
	            while (true) {
			    	try {
		                Map<String, SessionImpl> map = sessions;
		                synchronized (map) {
					    	for(String sessionID:map.keySet()) {
					    		SessionImpl session = map.get(sessionID);
				    			long lastAccessedTime = System.currentTimeMillis();
				    			if(lastAccessedTime-session.lastAccessedTime>1000*session.maxActiveInterval) {
				    				sessions.remove(sessionID);
					    		}
					    	}
							sleep(5000);
		                }
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	            }
		    }
	    };
		checkSession.start();
		
        serverLoop(serverSocket);
    }
    
    protected SessionImpl makeSession() {
        SessionImpl sessionImpl = new SessionImpl();
        Map<String, SessionImpl> map = this.sessions;
        synchronized (map) {
            this.sessions.put(sessionImpl.id(), sessionImpl);
        }
        return sessionImpl;
    }

    public Map<String, SessionImpl> getSessions() {
        return instance.sessions;
    }
}
