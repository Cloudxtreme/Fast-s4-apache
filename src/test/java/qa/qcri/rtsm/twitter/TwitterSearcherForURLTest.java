package qa.qcri.rtsm.twitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.util.RTSMConf;

public class TwitterSearcherForURLTest {

	static String url = "http://www.aljazeera.com/news/asia-pacific/2013/03/20133710449989855.html";
	static TwitterSearcher ts;
	
	@Before
	public void setUp() throws Exception {
		ts = new TwitterSearcher( RTSMConf.TEST_TWITTER_CONSUMER_KEY, RTSMConf.TEST_TWITTER_CONSUMER_SECRET, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN_SECRET );
	}

	/*
	@Test
	public void testTwitterSearcherForURL() {
		TwitterSearcherForURL searcher = new TwitterSearcherForURL(url, ts);
		assertTrue( searcher != null );
	}

	@Test
	public void testGetUrl() {
		TwitterSearcherForURL searcher = new TwitterSearcherForURL(url, ts);
		assertEquals( searcher.getUrl(), url );
	}
*/
	public static void main(String[] args) {
		TwitterSearcherForURL searcher = new TwitterSearcherForURL(url, ts);
		System.out.println(searcher.search());
	}
}
