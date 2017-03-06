package qa.qcri.rtsm.twitter;

import java.net.MalformedURLException;
import java.net.URL;

import org.json.JSONException;
import org.json.JSONObject;

import qa.qcri.rtsm.util.Util;
import qa.qcri.rtsm.util.WebUtil;

public class TwitterOpinionQuerier {
	String host;
	int port;
	
	public TwitterOpinionQuerier(String host, int port) {
		this.host = host;
		this.port = port;
		Util.logDebug(this, "Will use " + host + ":" + port + " for testing if a tweet is an opinion");
	}
	
	/**
	 * Returns a positive number if the tweet is an opinion, a negative number if not.
	 * 
	 * @param tweet The text of the tweet.
	 * @param title The title of the article being referenced.
	 * @return negative if not-opinion, 0.0 on error (e.g. back-end not reachable), positive if opinion
	 */
	double query(String tweet, String title) {
		String encodedTweetText = WebUtil.urlEncodeOrEmpty(tweet);
		String encodedTitle = WebUtil.urlEncodeOrEmpty(title);
		
		String file = "/?" + "tweetText=" + encodedTweetText + "&" + "title=" + encodedTitle;
		URL url = null;
		try {
			url = new URL("http", host, port, file );
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return 0.0;
		}
		String response = WebUtil.getHTMLContentsAsString(url.toString());
		if( response == null || response.length() == 0 ) {
			Util.logDebug(this, "Empty response");
			return 0.0;
		}
		JSONObject json;
		try {
			json = new JSONObject(response);
		} catch (JSONException e) {
			e.printStackTrace();
			return 0.0;
		}
		try {
			return json.getDouble("score");
		} catch (JSONException e) {
			e.printStackTrace();
			return 0.0;
		}
	}
}
