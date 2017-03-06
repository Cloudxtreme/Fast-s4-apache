package qa.qcri.rtsm.twitter;

import static org.junit.Assert.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.rtsm.analysis.TimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.twitter.TwitterTreeAnalyzer.Node;
import qa.qcri.rtsm.twitter.TwitterTreeAnalyzer.ParentAndDifference;

public class TwitterTreeAnalyzerTest {
	
	private TwitterTreeAnalyzer twitterTreeAnalyzer;

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testStripRT() {
		assertEquals( " without the RT part", TwitterTreeAnalyzer.stripRT("RT @hello: without the RT part") );
	}

	@Test
	public void testStripURLs() {
		assertEquals( "without the URL ", TwitterTreeAnalyzer.stripURLs("without the URL http://example.com/") );
	}

	@Test
	public void testStripRTandURLs() {
		assertEquals( " without RT or URL ", TwitterTreeAnalyzer.stripRTandURLs("RT @hello: without RT or URL http://example.com/") );
	}

	@Test
	public void testGetRTUsername() {
		assertEquals( "hello", TwitterTreeAnalyzer.getRTUsername("RT @hello: without the RT part") );
	}
	
	// Noora's test
	// parser exception //(expected=ParseException.class)
	@Test
	public void testAddString() throws ParseException, JSONException {
		/*
		Date date =new Date();
		System.out.println(date);
		System.out.println(DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).format(date));
		System.out.println(DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).parse("May 28, 2013 12:59:15 PM AST").getTime());
		*/
		
		// working date format: May 28, 2013 12:59:15 PM AST
		// not working date format: Wed May 01 18:03:50 AST 2013
		twitterTreeAnalyzer = new TwitterTreeAnalyzer();
		String test = "{\"createdAt\":\"May 28, 2013 12:59:15 PM AST\", \"id\":\"164312073092874240\", \"text\":\"Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)\", \"geoLocationStr\":\"null\", \"userLocation\":\"India\", \"userStatusesCount\":4, \"userFollowersCount\":5000, \"userFriendsCount\":300, \"fromUser\":\"sumit\", \"profileImageURL\":\"http://a0.twimg.com/profile_images/2163570068/hills_normal.jpg\"}";
		SimpleTweet simpleTweet = new SimpleTweet(test);
		twitterTreeAnalyzer.add(simpleTweet, "test");
		//
		// what should be compared!!
		assertTrue(simpleTweet !=null);
	}
	
	// parser exception //(expected=ParseException.class)
	@Test
	public void testAdd() throws ParseException, JSONException {
		twitterTreeAnalyzer = new TwitterTreeAnalyzer();
		// working date format: May 28, 2013 12:59:15 PM AST
		// not working date format: Wed May 01 18:03:50 AST 2013
		String test = "{\"createdAt\":\"Jan 31, 2012 02:40:25 PM AST\", \"id\":\"164312073092874240\", \"text\":\"Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)\", \"geoLocationStr\":\"null\", \"userLocation\":\"India\", \"userStatusesCount\":4, \"userFollowersCount\":5000, \"userFriendsCount\":300, \"fromUser\":\"sumit\", \"profileImageURL\":\"http://a0.twimg.com/profile_images/2163570068/hills_normal.jpg\"}";
		//
		SimpleTweet simpleTweet = new SimpleTweet(test);
		twitterTreeAnalyzer.add(simpleTweet);
		
		// what should be compared!!
		assertTrue(twitterTreeAnalyzer.tweets != null);
	}

	@Test
	public void testFindClosestNode() throws ParseException, JSONException {
		twitterTreeAnalyzer = new TwitterTreeAnalyzer();
		//
		// working date format: May 28, 2013 12:59:15 PM AST
		// not working date format: Wed May 01 18:03:50 AST 2013
		String test = "{\"createdAt\":\"Jan 31, 2012 02:40:25 PM AST\", \"id\":\"164312073092874240\", \"text\":\"Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)\", \"geoLocationStr\":\"null\", \"userLocation\":\"India\", \"userStatusesCount\":4, \"userFollowersCount\":5000, \"userFriendsCount\":300, \"fromUser\":\"sumit\", \"profileImageURL\":\"http://a0.twimg.com/profile_images/2163570068/hills_normal.jpg\"}";
		SimpleTweet simpleTweet = new SimpleTweet(test);
		
	    ParentAndDifference parentAndDifference = twitterTreeAnalyzer.findClosestNode(simpleTweet);
		assertTrue(parentAndDifference != null);
	}
	
	
	@Test
	public void testGetNoveltySeries() {
		// TODO: to be reviewed
		twitterTreeAnalyzer = new TwitterTreeAnalyzer();
		//TimeSeries ts = new TimeSeries("twitter-novelty");
		//ts.insertPoint(new Point(new Long(1), new Double(13)));
		
		TimeSeries ts = twitterTreeAnalyzer.getNoveltySeries();
		assertTrue(ts != null);
	}
}
