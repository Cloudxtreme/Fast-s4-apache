package qa.qcri.rtsm.item;

public class SiteConfiguration {
	protected String siteId;
	protected String URLPattern;
	protected String comment;
	protected String fbAuthToken;
	protected String twitterAuthToken;
	protected String twitterAuthTokenSecret;
	
	public String getSiteId() {
		return siteId;
	}
	public void setSiteId(String siteId) {
		this.siteId = siteId;
	}
	public String getURLPattern() {
		return URLPattern;
	}
	public void setURLPattern(String URLPattern) {
		this.URLPattern = URLPattern;
	}
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	public String getFbAuthToken() {
		return fbAuthToken;
	}
	public void setFbAuthToken(String fbAuthToken) {
		this.fbAuthToken = fbAuthToken;
	}
	public String getTwitterAuthToken() {
		return twitterAuthToken;
	}
	public void setTwitterAuthToken(String twitterAuthToken) {
		this.twitterAuthToken = twitterAuthToken;
	}
	public String getTwitterAuthTokenSecret() {
		return twitterAuthTokenSecret;
	}
	public void setTwitterAuthTokenSecret(String twitterAuthTokenSecret) {
		this.twitterAuthTokenSecret = twitterAuthTokenSecret;
	}
}
