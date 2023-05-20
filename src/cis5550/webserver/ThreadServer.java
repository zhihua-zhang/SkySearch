package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.FileInputStream;


import static cis5550.webserver.Server.*;

public class ThreadServer {
	public void workerThread() {
		byte[] EOH = {13,10,13,10};
		while (true) {
            Socket socket = null;
            try {
                socket = queue.take();
                boolean isClosed = false;
                boolean isBadRequest = false;
                boolean notImplemented = false;
                boolean notAllowed = false;
                boolean versionNotSupported = false;
                boolean forbidden = false;
                boolean notFound = false;
				boolean matched = false;

				while(!isClosed && !socket.isClosed()) {
					ByteArrayOutputStream buffer = new ByteArrayOutputStream();
					String sessionID = "";
				    // read bytes until double CRLF
				    int numBytesRead = 0, cur = 0;
				    boolean found = false;
				    while (!found) {
				      int b = 0;
				      b = socket.getInputStream().read();
				      if(b==-1) {
				    	  isClosed = true;
				    	  break;
				      }
				      if (b>=0) numBytesRead ++;
				      buffer.write(b);
				      if (b==EOH[cur]) {
				    	  cur++;
				    	  if(cur==4) found = true;
				      }
				      else cur = 0;
				    }
				    
					BufferedReader bufferReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.toByteArray())));
					DataOutputStream out = new DataOutputStream(socket.getOutputStream()); 

					HashMap<String,String> headers = new HashMap<String,String>();
					
					String line = bufferReader.readLine();
					System.out.println("Request: "+line);
					while(line!=null &&line.length()==0){
							line = bufferReader.readLine();
					}
					String[] split = line.split("\\s+");
					if(split.length!=3){
						System.out.println("Bad request");
						isBadRequest = true;
					}
					
					String method = split[0].trim();
					System.out.println("method: "+method);

					String URL = split[1].trim();
					System.out.println("URL: "+URL);

					String version = split[2].trim();
					System.out.println("version: "+version);
					
					line = "";
					System.out.println("Parsing headers");
					
					boolean end = false;
					
					while(!end){
						line = bufferReader.readLine();
						
						String[] split2 = line.split(":\\s*",2);
						if(split2.length>1){
							System.out.println("header: "+ line);

							String headerKey = split2[0].trim().toLowerCase();
							String headerVal = split2[1].trim();
							
							if(headerVal.contains("SessionID")) {
								String cookie = headerVal;

								String[] split1 = cookie.split(";\\s*");
								String curSessionID ="";
								if(split1.length==0) {
									String[] splitCookie = cookie.split("=");
									curSessionID = splitCookie[1];
									System.out.println("SessionID: "+ curSessionID);
								}else {
									for(String sp : split1) {
										if(sp.contains("SessionID")) {
											String[] splitCookie = sp.split("=");
											curSessionID = splitCookie[1];
											System.out.println("SessionID: "+ curSessionID);
										}
									}
								}
								
								
								if(!curSessionID.equals("") && instance.sessions.containsKey(curSessionID)) {
									SessionImpl session = instance.sessions.get(curSessionID);
									long lastAccessedTime = System.currentTimeMillis();
									session.setLastAccessedTime(lastAccessedTime);
									if(!session.valid) instance.sessions.remove(curSessionID);
									else{
										instance.sessions.put(curSessionID, session);
										sessionID = curSessionID;
									}
								}
							}

							if(!headers.containsKey(headerKey)){
								headers.put(headerKey,headerVal);
							}
						}else{
							end = true;
						}
											
					}
					
					if(!headers.keySet().contains("host")){
						isBadRequest = true;
					}
					
					byte byteBody[] = null;
					
					// read body
					if(headers.get("content-length")!=null){
						ByteArrayOutputStream body = new ByteArrayOutputStream();
						int length = Integer.parseInt(headers.get("content-length"));
					    for (int i=0; i<length; i++) {
					        int b = socket.getInputStream().read();
					        body.write(b);
					    }
					    byteBody = body.toByteArray();
					}
				    
				    if(isClosed) break;
				    
				    
				    String host = "default";
					if(headers.get("host")!=null) {
						host = headers.get("host");
						if(host.contains(":")) host = host.substring(0,host.indexOf(":"));
						System.out.println("host: "+host);
					}
					
					if(!instance.routes.keySet().contains(host)) host = "default";
					ArrayList<routeTuple> rt = instance.routes.get(host);

					for(routeTuple route : rt) {
						int indexParam = URL.indexOf("?");
						String URLwithoutParam = URL;
						if(indexParam!=-1){
							URLwithoutParam = URLwithoutParam.substring(0,indexParam);
						}
						HashMap<String,String> params = new HashMap<String,String>();

						if(route.method.equals(method) && match(URLwithoutParam,route.path,params)) {
							System.out.println("matched api");
							matched = true;
							HashMap<String, String> queryParams = new HashMap<>();
							if(URL.contains("?")) {
								String par = URL.substring(URL.indexOf("?")+1);
								queryParams(par, queryParams);

							}
									
							if(headers.keySet().contains("content-type") && headers.get("content-type").equals("application/x-www-form-urlencoded")) {
								String body = new String(byteBody, StandardCharsets.UTF_8);
								System.out.println("body params: "+body);
								queryParams(body, queryParams);
							}
							InetSocketAddress socketAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
							System.out.println("ip: "+socketAddress.getAddress().getHostAddress()+" port: "+socketAddress.getPort());

							RequestImpl newReq = new RequestImpl(method, URLwithoutParam, version, headers, queryParams, params, socketAddress, byteBody, instance);
							ResponseImpl newResp = new ResponseImpl(socket);
							newResp.header("Connection", "close");
							
							Object o = null;
							if(!sessionID.equals("")) {
								System.out.println("session not null");
								newReq.session = instance.sessions.get(sessionID);
							}
							
							try{
								o = route.L.handle(newReq, newResp);
							}catch(Exception e){
								e.printStackTrace();
								newResp.status(500, "Internal Server Error");
							}
							
							if(!newResp.writeCalled){
								String respInfo = "HTTP/1.1 "+newResp.statusCode+" "+newResp.reasonPhrase+"\r\n";
								out.write(respInfo.getBytes());
								System.out.println("respInfo: "+ respInfo);
								
								if(newReq.newSession) {
									newResp.header("Set-Cookie", "SessionID="+newReq.session.id());
								}
								
								if(o!=null) {
									String resp = o.toString();
									
									newResp.header("Content-Length", resp.length()+"");
									String respHeaders = newResp.returnHeaders;
									if(respHeaders.length()==0) respHeaders += "\r\n";
									System.out.println("resp headers: "+ headers);
									out.write((respHeaders+"\r\n").getBytes());
									
									out.write(resp.getBytes());
									System.out.println("resp body: "+ resp);

									out.flush();
								}else if(o==null) {
									int length = newResp.bodyRaw == null ? 0:newResp.bodyRaw.length;
									newResp.header("Content-Length", length+"");
									
									String respHeaders = newResp.returnHeaders;
									if(respHeaders.length()==0) respHeaders += "\r\n";
									System.out.println("resp headers: "+ headers);
									out.write((respHeaders+"\r\n").getBytes());
									
									if(newResp.bodyRaw != null)out.write(newResp.bodyRaw);
									out.flush();
								}
								
								break;
							}
							else {
								isClosed = true;
							}
						}
					}
					
					if(!matched) {
						if(!method.equals("GET") && !method.equals("HEAD") && !method.equals("POST") && !method.equals("PUT") ) {
							System.out.println("NotImplemented");
							notImplemented = true;
						}
						
						if(method.equals("POST") || method.equals("PUT") ) {
							System.out.println("NotAllowed");
							notAllowed = true;
						}
						
						if(!version.equals("HTTP/1.1")) {
							System.out.println("VersionNotSupported");
							versionNotSupported = true;
						}
						if(URL.contains("..")) {
							System.out.println("Forbidden");
							forbidden = true;
						}
						
								
						String filePath = staticFilePath.get(host)+""+URL;				
						
						System.out.println("----------Write Response----------");
						Utils utils = new Utils();
						StringBuilder response = new StringBuilder();
						
						
						// generate response code
						response.append("HTTP/1.1");
						
						File file = null;
						if(!isBadRequest && !notAllowed && !notImplemented && !versionNotSupported) {
							System.out.println("filePath here: "+filePath);

							file = new File(filePath); 
							if(!file.exists()) {
								System.out.println("NotFound");
								notFound = true;
							}
							else if(!file.canRead()) {
								System.out.println("Forbidden");
								forbidden = true;
							}
						}
						
						if(isBadRequest) {
							response.append(" "+utils.HTTP_BAD_REQUEST);
						}else if(forbidden) {
							response.append(" "+utils.HTTP_FORBIDDEN);
						}else if(notFound) {
							response.append(" "+utils.HTTP_NOT_FOUND);
						}else if(notAllowed) {
							response.append(" "+utils.HTTP_NOT_ALLOWED);
						}else if(notImplemented) {
							response.append(" "+utils.HTTP_NOT_IMPLEMENTED);
						}else if(versionNotSupported) {
							response.append(" "+utils.HTTP_NOT_SUPPORTED_VERSION);
						}else {
							response.append(" "+utils.HTTP_OK);
						}
						response.append("\r\n");
					
						
						System.out.println("-----generate resp headers-------");
						HashMap<String, String> respHeaders = new HashMap<>();

						respHeaders.put("Content-Length", "0");
						respHeaders.put("Content-Type", "application/octet-stream");

						if(!isBadRequest && !notFound && !forbidden && !notAllowed && !notImplemented && !versionNotSupported) {

							long bytes = file.length();
							respHeaders.put("Content-Length", bytes+"");
							
							if(filePath.contains(".jpg") || filePath.contains(".jpeg")) {
								respHeaders.put("Content-Type", "image/jpeg");
							}else if(filePath.contains(".txt")) {
								respHeaders.put("Content-Type", "text/plain");
							}else if(filePath.contains(".html")) {
								respHeaders.put("Content-Type", "text/html");
							}
							
						}else {
							respHeaders.put("Content-Length", "13");
						}
						respHeaders.put("Server", "Yaxin Liu's Server");
						
						for(String key : respHeaders.keySet()){
							response.append(key+": "+respHeaders.get(key)+"\r\n");
						}
						
						response.append("\r\n");
						
						

						out.write(response.toString().getBytes());
						out.flush();
						System.out.println("Send response headers: "+response.toString());
						
						
						// send file
						if(!isBadRequest && !notFound && !forbidden && !notAllowed && !notImplemented && !versionNotSupported) {
							if(method!=null && method.equals("GET")) {
								FileInputStream fileStream = new FileInputStream(file);
								byte file_buffer[] = new byte[1024];
								int num_bytes = 0;
										
								while ((num_bytes = fileStream.read(file_buffer)) != -1) {
									out.write(file_buffer, 0, num_bytes);
								}
								fileStream.close();
								System.out.println("file send");
							}
						}
						
						// send response body
						else {
							if(method==null || (method!=null && !method.equals("HEAD"))) {
								System.out.println("Response err body: 404 Not Found");
								out.write("404 Not Found".getBytes());
								out.flush();
							}
						}
					}

				}
				socket.close();

            }catch (Exception exception) {
                try {
                    socket.close();
                }
                catch (IOException iOException) {
                    // empty catch block
                }
                exception.printStackTrace();
            }
		
		}
	}
	
	
	public void queryParams(String par, HashMap<String, String> query) throws UnsupportedEncodingException{
		String[] splitPar = par.split("&");
		for(int i=0;i<splitPar.length;i++) {
			if(splitPar[i].length()>0) {
				int index = splitPar[i].indexOf("=");
				if(index!=-1) {
					query.put(URLDecoder.decode(splitPar[i].substring(0,index), "UTF-8"), URLDecoder.decode(splitPar[i].substring(index+1), "UTF-8"));
				}
			}
		}
	}
	
	public boolean match(String URL, String pattern,HashMap<String,String> params) {
		String[] splitURL = URL.split("\\/");
		String[] splitPattern = pattern.split("\\/");
		if(splitURL.length != splitPattern.length) return false;
		else {
			for(int i=0;i<splitURL.length;i++) {
				if(splitURL[i].length()>0 && splitPattern[i].length()>0) {
					if(!splitPattern[i].contains(":") && !splitPattern[i].equals(splitURL[i])) return false;
					if(splitPattern[i].contains(":")){
						String key = splitPattern[i].substring(1);
						try {
							params.put(key, URLDecoder.decode(splitURL[i],"UTF-8"));
		                }
		                catch (UnsupportedEncodingException unsupportedEncodingException) {}
//						params.put(key, splitURL[i]);
					}
				}
			}
		}
		return true;
	}

}
