package qa.qcri.rtsm.item;

import org.json.JSONObject;

import com.google.common.collect.ImmutableMap;

public class URLSeenFacebook extends URLSeen {
	
	long likes;
	long shares;
		
	public URLSeenFacebook() {

	}
	
	public URLSeenFacebook(String site, String url, long likes, long shares) {
		super(site, url);
		this.likes = likes;
		this.shares = shares;
	}
	
	@Override
	public String toString() {
		return (new JSONObject(ImmutableMap.of("site", site, "url", url, "likes", new Long(likes), "shares", new Long(shares)))).toString();
	}

	public long getLikes() {
		return likes;
	}

	public void setLikes(long likes) {
		this.likes = likes;
	}

	public long getShares() {
		return shares;
	}

	public void setShares(long shares) {
		this.shares = shares;
	}
}

