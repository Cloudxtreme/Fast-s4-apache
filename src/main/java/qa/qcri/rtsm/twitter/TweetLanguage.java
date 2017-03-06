package qa.qcri.rtsm.twitter;

public class TweetLanguage {
	private String tweetText;
	private String tweetLanguage;
	
	public TweetLanguage()
	{
		super();
	}

	public String getTweetText() {
		return tweetText;
	}

	public void setTweetText(String tweetText) {
		this.tweetText = tweetText;
	}

	public String getTweetLanguage() {
		return tweetLanguage;
	}

	public void setTweetLanguage(String tweetLanguage) {
		this.tweetLanguage = tweetLanguage;
	}	
}
