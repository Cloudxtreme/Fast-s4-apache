package qa.qcri.rtsm.facebooktmp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.facebook.FacebookQuerier;
import qa.qcri.rtsm.util.RTSMConf;

public class FacebookQuerierTempTest {
	
	FacebookQuerier fb;
	
//	@Before
	public void setUp() throws Exception {
		fb = new FacebookQuerier(RTSMConf.TEST_FACEBOOK_ACCESS_TOKEN);
	}

//	@Test
	public void test() throws MalformedURLException {
		assertEquals( RTSMConf.TEST_FACEBOOK_ACCESS_TOKEN_USER, fb.fetchUser("me").getUsername() );
	}
	
//	@Test
	public void testGetURLInfo() throws MalformedURLException {
		assertTrue( fb.getURLInfo("http://www.aljazeera.com/").getShare_count() > 0l );
	}

}
