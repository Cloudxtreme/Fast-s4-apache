package qa.qcri.rtsm.twitter;

import static org.junit.Assert.*;
import java.text.ParseException;
import org.apache.commons.lang.NotImplementedException;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

public class SimpleTweetTest {

	@Before
	public void setUp() throws Exception {
	}
	
	
/*
 // needs connection to get status
	@Test
	public void testSimpleTweetStatus() {
		fail("Not yet implemented");
	}
*/
	@Test(expected=NotImplementedException.class)
	public void testSimpleTweetSimpleTweet() {
		SimpleTweet simpleTweet = new SimpleTweet();
		simpleTweet.fromUser = "user";
		simpleTweet.profileImageURL = "http://a0.twimg.com/profile_images/1440863882/image_normal.jpg";
		simpleTweet.geoLocationStr = "place";
		simpleTweet.text = "this is a tweet";
		simpleTweet.userFollowersCount = 1000;
		simpleTweet.userFriendsCount = 200;
		simpleTweet.userStatusesCount = 5;
		simpleTweet.id = 123456789;
		simpleTweet.createdAt = 987654321;
		simpleTweet.userLocation = "city";
		//
		new SimpleTweet(simpleTweet);
	}

	@Test//(expected=ParseException.class)
	public void testSimpleTweetString() throws ParseException, JSONException {
		// working date format: May 28, 2013 12:59:15 PM AST
		// not working date format: Wed May 01 18:03:50 AST 2013
		String test = "{\"createdAt\":\"May 27, 2013 11:14:36 AM AST\", \"id\":\"164312073092874240\", \"text\":\"Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)\", \"geoLocationStr\":\"null\", \"userLocation\":\"India\", \"userStatusesCount\":4, \"userFollowersCount\":5000, \"userFriendsCount\":300, \"fromUser\":\"sumit\", \"profileImageURL\":\"http://a0.twimg.com/profile_images/2163570068/hills_normal.jpg\"}";
		SimpleTweet simpleTweet = new SimpleTweet(test);
		assertEquals("Cold wave in #Bhopal, today..  :(), IBO's r calling frm PUC after completing their vol's.This make the environment firedup. :-)", simpleTweet.getText());
	}

}