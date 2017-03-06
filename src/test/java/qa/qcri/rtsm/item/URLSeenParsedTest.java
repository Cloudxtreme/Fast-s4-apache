package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class URLSeenParsedTest {

	@Before
	public void setUp() throws Exception {
	}

	/**
	 * Make sure the constructor sets host automatically.
	 */
	@Test
	public void testURLSeenCounterStringString() {
		URLSeenParsed urlSeenParsed = new URLSeenParsed("www.example2.com", "http://www.example2.com/path/to/url.html?xxx", "<html><head><title>my title</title></head></html>");
		assertEquals( "www.example2.com", urlSeenParsed.getSite());
		assertEquals( "my title", urlSeenParsed.getTitle());
	}

}
