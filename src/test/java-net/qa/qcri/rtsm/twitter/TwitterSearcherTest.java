package qa.qcri.rtsm.twitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.item.URLSeenTweet;
import qa.qcri.rtsm.twitter.TwitterSearcher.TwitterSearcherInfo;
import qa.qcri.rtsm.util.RTSMConf;
import twitter4j.Status;
import twitter4j.TwitterException;

public class TwitterSearcherTest {

	TwitterSearcher ts;

	@Before
	public void setUp() throws Exception {
		ts = new TwitterSearcher(RTSMConf.TEST_TWITTER_CONSUMER_KEY, RTSMConf.TEST_TWITTER_CONSUMER_SECRET, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN,
				RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN_SECRET);
	}
	
	@Test
	public void testInvalidCredentials() {
		TwitterSearcher ts = new TwitterSearcher("invalid", "invalid", "invalid", "invalid");
		try {
			ts.getInfo();
			
			fail("Should have thrown an exception");

		} catch (TwitterException e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testLimit() throws TwitterException {
		TwitterSearcherInfo info = ts.getInfo();
		if (info.getLimit() < 180) {
			fail("The limit of '@" + info.getScreenName() + "' is less than 180, meaning that probably you are NOT using a correct token for authenticating.");
			
		}	
	}

	@Test
	public void testSearch() throws IOException {
		Vector<Status> results = ts.search("http://www.wikipedia.org/").getTweets();
		assertTrue(results.size() > 0);
		for (Status tweet : results) {
			URLSeenTweet urlSeenTweet = new URLSeenTweet("www.example.com", "http://www.example.com/", tweet);
			assertEquals(tweet.getText(), urlSeenTweet.getSimpleTweet().getText());
			assertEquals(tweet.getCreatedAt(), urlSeenTweet.getSimpleTweet().getCreatedAt());
			assertEquals(tweet.getUser().getScreenName(), urlSeenTweet.getSimpleTweet().getFromUser());
		}
	}
}
