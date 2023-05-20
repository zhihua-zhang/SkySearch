package cis5550.webserver;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import cis5550.tools.Logger;

public class SessionImpl implements Session {
	 String sessionID;
	 long creationTime;
	 long lastAccessedTime;
	 int maxActiveInterval = 300;
	 Map<String, Object> attributes;
	 boolean valid = true;
	
	public SessionImpl() {
		this.attributes = new HashMap<>();
		this.creationTime = System.currentTimeMillis();
		this.lastAccessedTime = System.currentTimeMillis();
		this.sessionID = generateSessionID();
	}
	
	public String generateSessionID() {
		String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890$&";
		StringBuilder sessionid = new StringBuilder();
		Random rnd = new Random();
        while (sessionid.length() < 20) {
            int index = (int) (rnd.nextFloat() * chars.length());
            sessionid.append(chars.charAt(index));
        }
        
    	System.out.println("generated session id: "+ sessionid.toString());

        return sessionid.toString();
	}
	

	public String id() {
		return this.sessionID;
	}

	public long creationTime() {
		return this.creationTime;
	}

	public long lastAccessedTime() {
		return this.lastAccessedTime;
	}

	public void setLastAccessedTime(long lastAccessedTime) {
		if(lastAccessedTime-this.lastAccessedTime>1000*this.maxActiveInterval) valid = false;
		else this.lastAccessedTime = lastAccessedTime;
	}

	public void maxActiveInterval(int seconds) {
		this.maxActiveInterval = seconds;
	}

	public void invalidate() {
		this.valid = false;
	}

	public Object attribute(String name) {
		return this.attributes.get(name);
	}

	public void attribute(String name, Object value) {
		this.attributes.put(name, value);
	}

}