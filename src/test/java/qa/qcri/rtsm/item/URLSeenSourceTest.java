package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URL;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.item.URLSeenSource.SourceType;

public class URLSeenSourceTest {
	
	Visit v1internal;
	
	Visit v2organic;
	
	Visit v3direct;
	
	Visit v4referral;
	
	String siteExample1 = "www.example.com";
	
	String siteExample2 = "www.example.com";
	
	String urlExample1 = "http://www.example.com/page1";
	
	String urlExample2 = "http://www.example.com/page2";
	
	String urlOther3 = "http://www.other.com/page1";

	@Before
	public void setUp() throws Exception {
		v1internal = new Visit();
		v1internal.setUrl(urlExample1);
		v1internal.setSource("(none)");
		v1internal.setSearchTerms("(not provided)");
		v1internal.setReferral(urlExample2);
		
		v2organic = new Visit();
		v2organic.setUrl(urlExample1);
		v2organic.setSource("google.com");
		v2organic.setSearchTerms("search terms");
		v2organic.setReferral("http://www.google.com/?q=search+terms");
		
		v3direct = new Visit();
		v3direct.setUrl(urlExample1);
		v3direct.setSource("(none)");
		v3direct.setSearchTerms("(not provided)");
		v3direct.setReferral("");
		
		v4referral = new Visit();
		v4referral.setUrl(urlExample1);
		v4referral.setSource((new URL(urlOther3)).getHost());
		v4referral.setSearchTerms("(not provided)");
		v4referral.setReferral(urlOther3);
		
	}

	@Test
	public void testURLSeenSource() {
		URLSeenSource u = new URLSeenSource();
		assertTrue( u != null );
	}

	@Test
	public void testURLSeenSourceStringVisit() {
		URLSeenSource u = new URLSeenSource(siteExample1, urlExample1, v1internal);
		assertTrue( u != null );
	}

	@Test
	public void testSourceType() {
		URLSeenSource u1 = new URLSeenSource(siteExample1, urlExample1, v1internal);
		assertEquals( SourceType.INTERNAL, u1.sourceType() );
		
		URLSeenSource u2 = new URLSeenSource(siteExample1, urlExample1, v2organic);
		assertEquals( SourceType.ORGANIC, u2.sourceType() );
		
		URLSeenSource u3 = new URLSeenSource(siteExample1, urlExample1, v3direct);
		assertEquals( SourceType.DIRECT, u3.sourceType() );
		
		URLSeenSource u4 = new URLSeenSource(siteExample1, urlExample1, v4referral);
		assertEquals( SourceType.REFERRAL, u4.sourceType() );
	}

	@Test
	public void testGetSource() {
		URLSeenSource u1 = new URLSeenSource(siteExample1, urlExample1, v1internal);
		assertEquals("(none)", u1.getSource() );
		URLSeenSource u2 = new URLSeenSource(siteExample1, urlExample1, v2organic);
		assertEquals("google.com", u2.getSource() );
	}

	@Test
	public void testGetSearchTerms() {
		URLSeenSource u1 = new URLSeenSource(siteExample1, urlExample1, v1internal);
		assertEquals("(not provided)", u1.getSearchTerms() );
		URLSeenSource u2 = new URLSeenSource(siteExample1, urlExample1, v2organic);
		assertEquals("search terms", u2.getSearchTerms() );
	}

	@Test
	public void testGetReferral() {
		URLSeenSource u1 = new URLSeenSource(siteExample1, urlExample1, v3direct);
		assertEquals("", u1.getReferral() );
		URLSeenSource u2 = new URLSeenSource(siteExample1, urlExample1, v4referral);
		assertEquals(urlOther3, u2.getReferral() );
	}
}
