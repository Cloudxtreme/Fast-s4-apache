package qa.qcri.rtsm.persist.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.item.URLSeenTweet;
import qa.qcri.rtsm.item.URLSeenTweetTest;
import qa.qcri.rtsm.twitter.SimpleTweet;

public class CassandraTweetTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testCassandraTweetString() {
		CassandraPersistentTweet ct = new CassandraPersistentTweet(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TWEETS);
		assertTrue( ct != null );
	}

	@Test
	public void testSetGet() {
		CassandraSchemasTest.clearKeyspace(CassandraSchemasTest.TEST_KEYSPACE);
		CassandraPersistentTweet ct = new CassandraPersistentTweet(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TWEETS);
		assertTrue( ct != null );
		
		URLSeenTweet ust1 = URLSeenTweetTest.getURLSeenTweetSample("site1", "url1", "username1", "text1", 42l);
		URLSeenTweet ust2 = URLSeenTweetTest.getURLSeenTweetSample("site1", "url1", "username2", "text2", 93l);
		URLSeenTweet ust3 = URLSeenTweetTest.getURLSeenTweetSample("site2", "url2", "username3", "text3", 1000000l);
		
		ct.set("url1", new Long(ust1.getSimpleTweet().getId()), ust1.getSimpleTweet() );
		ct.set("url1", new Long(ust2.getSimpleTweet().getId()), ust2.getSimpleTweet() );
		ct.set("url2", new Long(ust3.getSimpleTweet().getId()), ust3.getSimpleTweet() );
		
		TreeMap<Long, SimpleTweet> result1 = ct.get("url1");
		assertEquals( 2, result1.size() );
		assertEquals( "text2", result1.get(new Long(93)).getText() );
		assertEquals( "username1", result1.get(new Long(42)).getFromUser() );
		
		TreeMap<Long, SimpleTweet> result2 = ct.get("url2");
		assertEquals( 1, result2.size() );
		assertEquals( "text3", result2.get(new Long(1000000)).getText() );
		assertEquals( "username3", result2.get(new Long(1000000)).getFromUser() );
		
		TreeMap<Long, SimpleTweet> resultEmpty = ct.get("url3");
		assertEquals( 0, resultEmpty.size() );
	}
}
