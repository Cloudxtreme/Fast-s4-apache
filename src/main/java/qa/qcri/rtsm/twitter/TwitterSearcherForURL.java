package qa.qcri.rtsm.twitter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import org.json.JSONObject;

import qa.qcri.rtsm.util.LastWorkedTime;
import qa.qcri.rtsm.util.Util;
import twitter4j.Status;

import com.google.common.collect.ImmutableMap;

public class TwitterSearcherForURL implements LastWorkedTime {

	long lastCheckedTime;

	long lastTweetID;

	final String url;

	final TwitterSearcher ts;

	public TwitterSearcherForURL(String urlIn, TwitterSearcher ts) {
		this.lastCheckedTime = Long.MIN_VALUE;
		this.lastTweetID = -1;
		this.ts = ts;
		String url = urlIn.replaceFirst("https?://", "");

		if(url.contains("www.aljazeera.net")){
			Util.logDebug(this, "arabic url to Twittwer search "+url);
			String[] tokensRaw = url.split("/");
			ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
			for( int i=0; i<tokensRaw.length; i++ ) {
				try {
					tokensEncoded.add(i, URLDecoder.decode(tokensRaw[i], Util.UTF8.toString()));
				} catch (UnsupportedEncodingException e) {
					tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());
				}	
			}
			url = StringUtils.join(tokensEncoded, "/");
			Util.logDebug(this, "Decoded Arabic url "+url);
		}

		//if(!url.contains("www.aljazeera.net")){
			String[] tokensRaw = url.split("/");
			ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
			for( int i=0; i<tokensRaw.length; i++ ) {
				try {
					tokensEncoded.add(i, URLEncoder.encode(tokensRaw[i], Util.UTF8.toString()));
				} catch (UnsupportedEncodingException e) {
					tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());
				}	
			}
			String url1 = "http://"+StringUtils.join(tokensEncoded, "/");
                        if(url1.contains("dohanews.co") && !url1.endsWith("/"))
	                               url1= url1+"/";
		      this.url=url1;

		//}
		//else{
			/*
			String[] tokensRaw = url.split("/");
			ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
			for( int i=0; i<tokensRaw.length; i++ ) {
				try {
					//tokensEncoded.add(i, URLDecoder.decode(tokensRaw[i], Util.UTF8.toString()));
					tokensEncoded.add(i, URLEncoder.encode(tokensRaw[i], Util.UTF8.toString()));
				} catch (UnsupportedEncodingException e) {
				     tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());	
				}
			}
			this.url = "http://"+StringUtils.join(tokensEncoded, "/");
			*/
		//	this.url = "http://"+url;
		//}
	}

/*    public TwitterSearcherForURL(String str) {
       	String URL="";
	String url = str.replaceFirst("https?://", "");
        if(url.contains("www.aljazeera.net")){
                String[] tokensRaw = url.split("/");
                ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
               for( int i=0; i<tokensRaw.length; i++ ) {
                       try {
                                tokensEncoded.add(i, URLEncoder.encode(tokensRaw[i], Util.UTF8.toString()));
                     } catch (UnsupportedEncodingException e) {
                              tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());
                     }
               }
               URL = StringUtils.join(tokensEncoded, "/");
       }

	else{
		URL = url;

	}
        this.url= "http://"+URL;
    }

*/

	/**
	 * @return tweets sorted from newer to older
	 */
	public synchronized Vector<Status> search() {
		Vector<Status> results;

		if (lastTweetID != -1) {
			Util.logInfo(this, "Non-first twitter search for '" + url + "' after tweetID " + lastTweetID);
			results = ts.search(url, lastTweetID).getTweets();
		} else {

			Util.logInfo(this, "First twitter search for '" + url + "'");
			results = ts.search(url).getTweets();
		}
		Util.logInfo(this, "Got " + results.size() + " results for '" + url + "'");

		if (results.size() > 0) {

			// Sort results by descending tweetID
			Collections.sort(results, new Comparator<Status>() {
				@Override
				public int compare(Status t1, Status t2) {
					long i1 = t1.getId();
					long i2 = t2.getId();
					if (i1 < i2) {
						return 1;
					} else if (i1 > i2) {
						return -1;
					} else {
						return 0;
					}
				}
			});

			// Check that order is indeed descending (no duplicates)
			long lastId = Long.MAX_VALUE;
			for (Status tweet : results) {
				long id = tweet.getId();
				if (id >= lastId) {
					throw new IllegalStateException("Tweets were not sorted in strict ascending order [according to id] " + id + ">=" + lastId );
				}
				lastId = id;
			}

			// Check that the last one in the set (the older tweet) is newer than the lastTweetID
			if (lastTweetID != -1) {
				if (lastId <= lastTweetID) {
					throw new IllegalStateException(ts.getClass().getName() + " returned tweets older than " + lastTweetID);
				}
			}

			// The newer is the first one
			lastTweetID = results.get(0).getId();
		}

		this.lastCheckedTime = System.currentTimeMillis();

		if( ! hasWorked() ) {
			throw new IllegalStateException("Just did a search, hasWorked() is returning false" );
		}
		
		return results;
	}
	
	public boolean hasWorked() {
		return (this.lastCheckedTime != Long.MIN_VALUE);
	}

	public long getLastWorkedTime() {
		return lastCheckedTime;
	}

	public String getUrl() {
		return url;
	}
	
	public String toString() {
		return (new JSONObject(ImmutableMap.of( "ts", ts, "url", url ))).toString();
	}
}
