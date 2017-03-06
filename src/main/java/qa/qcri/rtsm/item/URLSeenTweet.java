package qa.qcri.rtsm.item;

import org.json.JSONException;
import org.json.JSONObject;

import qa.qcri.rtsm.twitter.SimpleTweet;
import twitter4j.Status;

public class URLSeenTweet extends URLSeen {

	SimpleTweet tweet;
	
	public URLSeenTweet() {

	}
	
	public URLSeenTweet(String site, String url, Status tweet) {
		super(site, url);
		this.tweet = new SimpleTweet(tweet);
	}
	
	@Override
	public String toString() {
		JSONObject json = new JSONObject();
		try {
			json.put("site", site);
			json.put("url", url);
			json.put("tweet", getSimpleTweet().toJSON());
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return json.toString();
	}

	public SimpleTweet getSimpleTweet() {
		return tweet;
	}

	public void setTweet(SimpleTweet tweet) {
		this.tweet = tweet;
	}
}

