package qa.qcri.rtsm.twitter;

import static org.junit.Assert.*;

import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.twitter.Blacklist;

public class BlacklistTest {

	private Blacklist bl;
	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testTweetContainsBlacklistTermString() {
	// public void test() {
		bl = new Blacklist();
		ConcurrentHashMap<String, Double> blacklistWords = bl.blacklistWords;
		
		String test1 = "4 tornado warnings entire family is in a town where a tornado warning is in effect  #whatthehell";
		String result1 = bl.tweetContainsBlacklistTermString(test1, blacklistWords);
		assertEquals(null, result1);	
		
		String test2 = "That's tough shit RT @PublicityHound Red Cross needs blood donors. 300+ blood drives canceled due to hurricane.";
		String result2 = bl.tweetContainsBlacklistTermString(test2, blacklistWords);
		assertEquals("shit", result2);
	}
	
	@Test
	public void testTweetContainsBlacklistTerm() {
	// public void test1() {
		bl = new Blacklist();
		ConcurrentHashMap<String, Double> blacklistWords = bl.blacklistWords;
		
		String test1 = "4 tornado warnings entire family is in a town where a tornado warning is in effect  #whatthehell";
		boolean result1 = bl.tweetContainsBlacklistTerm(test1, blacklistWords);
		assertEquals(false, result1);	
		
		String test2 = "That's tough shit RT @PublicityHound Red Cross needs blood donors. 300+ blood drives canceled due to hurricane.";
		boolean result2 = bl.tweetContainsBlacklistTerm(test2, blacklistWords);
		assertEquals(true, result2);
	}


}