package qa.qcri.rtsm.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;

public class Blacklist {

	public static final String filename = "blacklist.txt";
	public ConcurrentHashMap<String, Double> blacklistWords = new ConcurrentHashMap<String, Double>();
	
	public Blacklist() {
		String words = "";
		try {
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream(filename);
			if( stream == null ) {
				throw new IllegalStateException("Couldn't find the resource '" + filename + "'");
			}
			while (stream.available() > 0) 
			{
				words += (char)stream.read();
			}
			for(String term: words.split(", ")){
				blacklistWords.put(new String(term.replaceAll(" ","")), new Double(1));
			}
		}
		catch (Exception e) {
			 System.err.println("Error reading file: " + e);
		}
	}
	
	public boolean tweetContainsBlacklistTerm(String tweet, ConcurrentHashMap<String, Double> blacklist) {
		
		tweet = tweet.toLowerCase();
		for(String term: tweet.split(" ")){
			if(blacklist.containsKey(term)) {
				return true;
			}
		}
		return false;
	}

	public String tweetContainsBlacklistTermString(String tweet, ConcurrentHashMap<String, Double> blacklist) {
		
		tweet = tweet.toLowerCase();
		for(String term: tweet.split(" ")){
			if(blacklist.containsKey(term)) {
				return term;
			}
		}
		return null;
	}
}