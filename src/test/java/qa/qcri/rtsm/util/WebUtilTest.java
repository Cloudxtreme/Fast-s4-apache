package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import net.htmlparser.jericho.Source;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;


public class WebUtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetTitleOfParsedHTML() throws ParseException {
		Source source = WebUtil.getParsedHTML("<html><head><title>xyz</title></head><body></body></html>");
		assertTrue( WebUtil.getTitleOfParsedHTML(source).equals("xyz") );
	}
	
	@Test
	public void testGetOGImageOfParsedHTML() throws ParseException {
		Source source;
		
		source = WebUtil.getParsedHTML("<html><head><title>xyz</title><meta property=\"og:image\" content=\"abc\"/></head><body></body></html>");
		assertTrue( WebUtil.getOGImageOfParsedHTML(source).equals("abc") );
		
		source = WebUtil.getParsedHTML("<html><head><title>xyz</title></head><body></body></html>");
		assertTrue( WebUtil.getOGImageOfParsedHTML(source).equals("") );
		
	}
	
//	@Test
//	public void testUrlToFilenameString() {
//		assertEquals( "example.com/news/americas/2012/11/20121159%273226787271.html", WebUtil.urlToFilename("http://example.com/news/americas/2012/11/20121159'3226787271.html") );
//	}
	
	@Test
	public void testGetStrippedNormalizedURL() {
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with?query"));
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with?query#fragment"));
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with#fragment"));
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with#"));
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with "));
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with;"));
		assertEquals("http://www.example.com/with/path.html", WebUtil.getStrippedNormalizedUrl("http://www.example.com/with/path.html"));
		assertEquals("http://www.example.com/with", WebUtil.getStrippedNormalizedUrl("http://WWW.EXAMPLE.COM/with;"));
	}
	
	@Test
	public void testCheckURLHostContains() {
		assertTrue( WebUtil.checkURLHostContains( "http://www.example.com/long/url", "example.com" ) );
		assertTrue( WebUtil.checkURLHostContains( "http://other.example.com/long/url", "example.com" ) );
		assertFalse( WebUtil.checkURLHostContains( "http://example1com/long/url", "example.com" ) );
		assertFalse( WebUtil.checkURLHostContains( "http://www.other.com/long/url/?example.com", "example.com" ) );
		assertFalse( WebUtil.checkURLHostContains( "http://www.other.com/long/url/example.com", "example.com" ) );
		assertFalse( WebUtil.checkURLHostContains( "http://www.other.com/long/url/", "example.com" ) );
	}
	
	// Noora's test
	@Test
	public void urlEncodeOrEmptyString()
	{
		String test = "how people in RI can help Missouri tornado victims http://tf.to/sXye";
		assertEquals("how+people+in+RI+can+help+Missouri+tornado+victims+http%3A%2F%2Ftf.to%2FsXye", WebUtil.urlEncodeOrEmpty(test));
		//
		String testEmpty = "";
		assertEquals("", WebUtil.urlEncodeOrEmpty(testEmpty));
		//
		String testNoURL = "this is a test for encode";
		assertEquals("this+is+a+test+for+encode", WebUtil.urlEncodeOrEmpty(testNoURL));
	}
}
