package qa.qcri.rtsm.twitter;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.nlpTools.TweetAnnotation;
import qa.qcri.rtsm.nlpTools.TweetAnnotator;

public class TweetAnnotatorTest {

	private TweetAnnotator ta;
	
	@Before
	public void setUp() throws Exception {
	}

	/*
	@Test
	public void testTweetAnnotator() {
		fail("Not yet implemented");
	}
	*/

	//@Test
	public void testAnnotate() {
		ta = new TweetAnnotator();
		TweetAnnotation tweetAnnotation = new TweetAnnotation();
		// "null	[]	[]	[txwx]	[Temp, Â, F DP, Â, Hum, % Bar, inHg Steady, Wind SSW, G, Rain]";
		// need to fix the empty arrays, empty arrays != null arrays
		tweetAnnotation.dates = null;
		ArrayList<String> locArrayList = new ArrayList<String>();
		tweetAnnotation.locations = locArrayList;
		ArrayList<String> menArrayList = new ArrayList<String>();
		tweetAnnotation.mentions = menArrayList;
		//
		ArrayList<String> htArrayList = new ArrayList<String>();
		htArrayList.add("txwx");
		tweetAnnotation.hashtags = htArrayList;
		//
		ArrayList<String> names = new ArrayList<String>();
		names.add("Temp");
		names.add("Â");
		names.add("F DP");
		names.add("Â");
		names.add("Hum");
		names.add("% Bar");
		names.add("inHg Steady");
		names.add("Wind SSW");
		names.add("G");
		names.add("Rain");
		tweetAnnotation.names = names;
		//
		TweetAnnotation tas = ta.annotate("01/06/12 05:12 Temp 74.4Â°F DP 70.6Â° Hum 88% Bar. 29.850 inHg Steady,  Wind SSW @ 0 G 1 Rain 0.00, Changeable, mending #txwx");
		assertEquals(tweetAnnotation.dates, tas.dates);
		assertEquals(tweetAnnotation.locations, tas.locations);
		assertEquals(tweetAnnotation.mentions, tas.mentions);
		assertEquals(tweetAnnotation.hashtags, tas.hashtags);
		assertEquals(tweetAnnotation.names, tas.names);
		
		
	}

}