package cis5550.webserver;

import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ResponseImpl implements Response{
	int statusCode=200;
	String reasonPhrase = "OK";
	byte bodyRaw[];
	String returnHeaders="";
	boolean contentType = false;
	boolean writeCalled = false;
	Socket sock;
	

	public ResponseImpl(Socket sock) {
		this.sock = sock;
	}

	public void body(String body) {
		this.bodyRaw = body.getBytes();
	}

	public void bodyAsBytes(byte[] bodyArg) {
		this.bodyRaw = bodyArg;
	}

	public void header(String name, String value) {	
		returnHeaders += name + ": " + value+"\r\n";
	}

	public void type(String contentType) {	
		if(!this.contentType) {
			returnHeaders += "Content-Type: " + contentType+"\r\n";
		}else {
			String[] split = returnHeaders.split("\r\n");
			String newHeaders = "";
			for(String sp:split) {
				if(sp.length()>0) {
					if(!sp.toLowerCase().contains("content-type:")) newHeaders += sp + "\r\n";
					else newHeaders += "Content-Type: " + contentType + "\r\n";
				}
			}
			this.contentType = true;
			returnHeaders = newHeaders;
		}
	}

	public void status(int statusCode, String reasonPhrase) {	
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}

	public void write(byte[] b) throws Exception {
		DataOutputStream out = new DataOutputStream(this.sock.getOutputStream()); 

		if(!this.writeCalled) {
			this.returnHeaders += "Connection: close\r\n";
			out.write("HTTP/1.1".getBytes());
			out.write((" "+this.statusCode+" "+this.reasonPhrase+"\r\n").getBytes());
			
			String headers = this.returnHeaders;
			if(headers.length()==0) headers += "\r\n";
			out.write((headers+"\r\n").getBytes());
			out.flush();
		}
		out.write(b);
		out.flush();
		this.writeCalled = true;
	}

	public void redirect(String url, int responseCode) {	
		this.statusCode = responseCode;
		switch(responseCode) {
			case 301:
				this.reasonPhrase = "Moved Permanently";
				break;
			case 302:
				this.reasonPhrase = "Found";
				break;
			case 303:
				this.reasonPhrase = "See Other";
				break;
			case 307:
				this.reasonPhrase = "Temporary Redirect";
				break;
			case 308:
				this.reasonPhrase = "Permanent Redirect";
				break;
			default:
				this.reasonPhrase = "";
		}
		
		returnHeaders += "Location: " + url+"\r\n";
	}

	public void halt(int statusCode, String reasonPhrase) {		
	}
	
	

}