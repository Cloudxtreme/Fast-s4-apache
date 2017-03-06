package qa.qcri.rtsm.twitter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.util.RTSMConf;
import twitter4j.Status;

public class TwitterSearcherForURLTestNet {

	String url = "www.aljazeera.com";
	TwitterSearcher ts;
	
	@Before
	public void setUp() throws Exception {
		ts = new TwitterSearcher( RTSMConf.TEST_TWITTER_CONSUMER_KEY, RTSMConf.TEST_TWITTER_CONSUMER_SECRET, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN_SECRET );
	}

	@Test
	public void testHasSearched() {
		TwitterSearcherForURL searcher = new TwitterSearcherForURL(url, ts);
		assertFalse( searcher.hasWorked() );
		searcher.search();
		assertTrue( searcher.hasWorked() );
	}

	@Test
	public void testSearch() throws InterruptedException {
		TwitterSearcherForURL searcher = new TwitterSearcherForURL(url, ts);
		assertFalse( searcher.hasWorked() );
		
		List<Status> oldTweets = searcher.search();
		assertTrue( searcher.hasWorked() );
		
		assertTrue( oldTweets.size() > 0 );
		assertOrderedNewerToOlder( oldTweets );
		System.err.println( "Old tweets: " + oldTweets.size() );
				
		// Sleep 3 secs
		Thread.sleep(3000);
		
		List <Status> newTweets = searcher.search();
		assertTrue( searcher.hasWorked() );
		System.err.println( "New tweets: " + newTweets.size() );
		
		if( newTweets.size() > 0 ) {
			
			// Check order
			assertOrderedNewerToOlder( newTweets );
			
			// Check there are no repetitions
			HashSet<Long> seen = new HashSet<Long>();
			for( Status tweet: oldTweets ) {
				seen.add( new Long(tweet.getId()) );
			}
			for( Status tweet: newTweets ) {
				assertFalse( seen.contains(new Long(tweet.getId())));
			}
			
			// Check these sets do not overlap in time
			assertTrue( getSmallerId(newTweets) > getLargerId(oldTweets) ); 
		}
		
	}
	
	private void assertOrderedNewerToOlder(List<Status> tweets) {
		long lastId = Long.MAX_VALUE;
		for( Status tweet: tweets ) {
			long currentId = tweet.getId();
			assertTrue( currentId < lastId );
			lastId = currentId;
		}
	}
	
	private long getSmallerId(List<Status> tweets) {
		long smallerId = Long.MAX_VALUE;
		for( Status tweet: tweets ) {
			long id = tweet.getId();
			if( id < smallerId ) {
				smallerId = id;
			}
		}
		return smallerId;
	}
	
	private long getLargerId(List<Status> tweets) {
		long largerId = Long.MIN_VALUE;
		for( Status tweet: tweets ) {
			long id = tweet.getId();
			if( id > largerId ) {
				largerId = id;
			}
		}
		return largerId;
	}

	@Test
	public void testGetLastCheckedTime() {
		TwitterSearcherForURL searcher = new TwitterSearcherForURL(url, ts);
		assertFalse( searcher.hasWorked() );
		
		Date currentDate = new Date();
		
		searcher.search();
		assertTrue( searcher.hasWorked() );
		
		assertTrue( searcher.getLastWorkedTime() >= currentDate.getTime() );
	}
}
