package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import net.htmlparser.jericho.Source;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

public class WebUtilTestNet {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetTitleOfUrl() throws ParseException {
		Source source = WebUtil.getParsedHTML(WebUtil.getHTMLContentsAsString("http://www.yahoo.com/"));
		assertTrue(WebUtil.getTitleOfParsedHTML(source).contains("Yahoo"));
	}

	@Test
	public void testGetHTMLContentsAsString() {
		assertTrue(WebUtil.getHTMLContentsAsString("thiscontainsnohost") == null);
		assertTrue(WebUtil.getHTMLContentsAsString("ftp://example.com/example/") == null);
		assertTrue(WebUtil.getHTMLContentsAsString("http://invalidhost.invalidhost/") == null);
		assertTrue(WebUtil.getHTMLContentsAsString("http://www.google.com/").contains("Google"));
	}
	
	// Noora's test
	@Test
	public void getTopPrivateDomainString()
	{
		String url1 = "http://tf.to/sXye";
		assertEquals("tf.to", WebUtil.getTopPrivateDomain(url1));
		
		String url2 = "www.google.com";
		assertEquals("malformed-url", WebUtil.getTopPrivateDomain(url2));
		
		String url3 = "http://www.google.com";
		assertEquals("google.com", WebUtil.getTopPrivateDomain(url3));
		
		String url4 = "http://google.com";
		assertEquals("google.com", WebUtil.getTopPrivateDomain(url4));
		
		String url5 = "";
		assertEquals("malformed-url", WebUtil.getTopPrivateDomain(url5));
		
		String url6 = null;
		assertEquals("malformed-url", WebUtil.getTopPrivateDomain(url6));
		
		String url7 = "http://";
		assertEquals("empty-host", WebUtil.getTopPrivateDomain(url7));
	}
}