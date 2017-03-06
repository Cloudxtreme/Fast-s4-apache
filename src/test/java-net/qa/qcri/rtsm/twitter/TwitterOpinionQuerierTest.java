package qa.qcri.rtsm.twitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.util.RTSMConf;

public class TwitterOpinionQuerierTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGet() {
		TwitterOpinionQuerier querier = new TwitterOpinionQuerier(RTSMConf.TWITTER_OPINION_HOST, RTSMConf.TWITTER_OPINION_PORT);
		assertTrue(querier != null);

		String tweetText;
		String title;

		title = "BBC World Service cuts outlined to staff";
		tweetText = "What a time for UK to choose to reduce its trusted voice RT @vali_nasr: Terrible news: BBC to Trim World Service - http://nyti.ms/fF8VyS";

		// Check for zero
		if (querier.query(tweetText, title) == 0.0) {
			fail("It seems the Twitter-Opinion back-end is not running on " + RTSMConf.TWITTER_OPINION_HOST + ":" + RTSMConf.TWITTER_OPINION_PORT);
		}

		assertEquals(0.0329, querier.query(tweetText, title), 1e-2);

		title = "BBC World Service cuts outlined to staff";
		tweetText = "U.S. embassy asks Pakistan to release diplomat http://bit.ly/folcta";
		assertEquals(-2.0490, querier.query(tweetText, title), 1e-2);

		title = "BBC World Service cuts outlined to staff";
		tweetText = "NDTV: US asks Pakistan to release diplomat http://snipurl.com/1xerb8";
		assertEquals(-2.1512, querier.query(tweetText, title), 1e-2);
	}
}
