package qa.qcri.rtsm.analysis;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.*;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.twitter.SimpleTweet;

public class TweetOfflineAnalyzerTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testIsRetweetOf() {
		HashSet<String> accounts = new HashSet<String>();
		accounts.add("ajenglish");
		accounts.add("ajelive");
		
		String t1 = "RT @AJEnglish: \"Entrepreneurship is a buzzword, but that's what built America. In the long run, it's not the rich guy or the poor guy\" http://t.co/VV2mb6zF";
		String t2 = "RT @AJELive: RT @AJELive: Al Jazeera's @AJEchris finds out from Venezuela's voters in their own words: http://t.co/JlrO3aYa #Venezuela #Capriles #Chavez";
		String t3 = "Yet another cowardly attack!! RT @AJELive: Deadly suicide bomb rocks #Pakistan town : http://t.co/3KT5ExaW";
		
		assertTrue( TweetOfflineAnalyzer.isRetweetOf(t1, accounts) );
		assertTrue( TweetOfflineAnalyzer.isRetweetOf(t2, accounts) );
		assertTrue( TweetOfflineAnalyzer.isRetweetOf(t3, accounts) );
		
		String t4 = "RT @TSICLondon: \"I have rights. I have the right of education\" #Malala, the classrooms are still full, girls refuse to be intimidated http://t.co/byvtu8dS";
		String t5 = "RT @AsadHashim: Latest dispatch: Children whose parents were killed by Taliban fight back by getting an education in Swat: http://t.co/tm1yqzXS #Malala";
		String t6 = "The fight for education in Pakistan's Swat http://t.co/In5wLQsQ";
		assertFalse( TweetOfflineAnalyzer.isRetweetOf(t4, accounts) );
		assertFalse( TweetOfflineAnalyzer.isRetweetOf(t5, accounts) );
		assertFalse( TweetOfflineAnalyzer.isRetweetOf(t6, accounts) );
	}
	
	@Test
	public void testentropy() {
		HashMap<String, Long> test = new HashMap<String, Long>();
		test.put("key1", new Long(9));
		test.put("key2", new Long(1));
		
		double result = TweetOfflineAnalyzer.entropy(test);
		assertEquals(0.4689955935892812, result, 0);
	}
	
	@Test
	public void testReadTweets() throws ParseException, JSONException {
		// working date format: May 28, 2013 12:59:15 PM AST
		// not working date format: Wed May 01 18:03:50 AST 2013
		List<String> lines = Arrays.asList("{test	{\"createdAt\":\"May 27, 2013 11:14:36 AM AST\", \"id\":\"164312073092874240\", \"text\":\"Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)\", \"geoLocationStr\":\"null\", \"userLocation\":\"India\", \"userStatusesCount\":4, \"userFollowersCount\":5000, \"userFriendsCount\":300, \"fromUser\":\"sumit\", \"profileImageURL\":\"http://a0.twimg.com/profile_images/2163570068/hills_normal.jpg\"}}", "{test1	{\"createdAt\":\"May 27, 2013 11:14:36 AM AST\", \"id\":\"164312073092874240\", \"text\":\"Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)\", \"geoLocationStr\":\"null\", \"userLocation\":\"India\", \"userStatusesCount\":4, \"userFollowersCount\":5000, \"userFriendsCount\":300, \"fromUser\":\"sumit\", \"profileImageURL\":\"http://a0.twimg.com/profile_images/2163570068/hills_normal.jpg\"}}");
		TweetOfflineAnalyzer.tweets = new Vector<SimpleTweet>(lines.size());
		TweetOfflineAnalyzer.dates = new Vector<Long>(lines.size());
		TweetOfflineAnalyzer.readTweets(lines,"test");
		
		Long number = new Long("1369642476000");
		assertEquals(number, TweetOfflineAnalyzer.dates.get(0));
	}
	
	@Test
	public void testcomputeWordsEntropySeries() {
		try {
			testReadTweets();
		} catch (ParseException | JSONException e) {
			e.printStackTrace();
		}
		TimeSeries tsEntropy = TweetOfflineAnalyzer.computeWordsEntropySeries();
		TimeSeries ts = new TimeSeries("test");
		ts.insertOrReplacePoint(new Point(new Long("1369642476000"),new Double(4.39231742277876)));
		ts.insertOrReplacePoint(new Point(new Long("1369642476000"),new Double(4.39231742277876)));
		//
		assertEquals(ts.sumValues(), tsEntropy.sumValues());
	}
	
	@Test
	public void testComputeUniqueTweetsSeries() {
		try {
			testReadTweets();
		} catch (ParseException | JSONException e) {
			e.printStackTrace();
		}
		TimeSeries result1 = TweetOfflineAnalyzer.computeUniqueTweetsSeries(false);
		TimeSeries ts1 = new TimeSeries("test");
		ts1.insertOrReplacePoint(new Point(new Long("1369642476000"), new Double(1.0)));
		ts1.insertOrReplacePoint(new Point(new Long("1369642476000"), new Double(1.0)));
		//
		assertEquals(ts1.sumValues(), result1.sumValues());
		//
		TimeSeries result2 = TweetOfflineAnalyzer.computeUniqueTweetsSeries(true);
		TimeSeries ts2 = new TimeSeries("test");
		ts2.insertOrReplacePoint(new Point(new Long("1369642476000"), new Double(1.0)));
		ts2.insertOrReplacePoint(new Point(new Long("1369642476000"), new Double(0.5)));
		//
		assertEquals(ts2.sumValues(), result2.sumValues());
	}
	
	@Test
	public void testCorporateRetweetsFraction() {
		// TODO reviewed
		try {
			testReadTweets();
		} catch (ParseException | JSONException e) {
			e.printStackTrace();
		}
		TimeSeries result = TweetOfflineAnalyzer.corporateRetweetsFraction("corporate,corporate2,corporate3,corporate4");
		TimeSeries ts = new TimeSeries("test");
		assertEquals(ts.sumValues(), result.sumValues());
	}
}