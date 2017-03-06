package qa.qcri.rtsm.process;

import qa.qcri.rtsm.item.URLSeenTweet;
import qa.qcri.rtsm.persist.TweetPersister;
import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.util.Util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

public class TweetPreparePersistPE extends PersistedAbstractPE {
	
    private TweetPersister tweetPersister;
    
	public void processEvent(URLSeenTweet urlSeenTweet) {
		String url = urlSeenTweet.getUrl();
		SimpleTweet tweet = urlSeenTweet.getSimpleTweet();


		if(url.contains("www.aljazeera.net")){
			url = url.replaceFirst("https?://", "");
			String[] tokensRaw = url.split("/");
			ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
			for( int i=0; i<tokensRaw.length; i++ ) {
				try {
					tokensEncoded.add(i, URLDecoder.decode(tokensRaw[i], Util.UTF8.toString()));
				} catch (UnsupportedEncodingException e) {
					tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());
																																						}
			}
			url="http://"+StringUtils.join(tokensEncoded, "/");
		}

		Util.logDebug(this, "Got for '" + url + "': " + tweet.toString() );
		tweetPersister.set(url, tweet);
    }

    @Override
    public void output() {
    	//No-op
    }
    
	@Override
	public void initInstance() {
		super.initInstance();
    	tweetPersister = (TweetPersister)persister;
	}

}
