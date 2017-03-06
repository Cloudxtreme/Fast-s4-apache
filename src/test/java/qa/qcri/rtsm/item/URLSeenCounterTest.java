package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class URLSeenCounterTest {

	@Before
	public void setUp() throws Exception {
	}

	/**
	 * Make sure the constructor sets host automatically.
	 */
	@Test
	public void testURLSeenCounterStringInt() {
		URLSeenCounter urlSeenCounter;
		
		urlSeenCounter = new URLSeenCounter( "www.example2.com", "http://www.example2.com/path/to/url.html?xxx", 10, 1001);
		assertEquals( "www.example2.com", urlSeenCounter.getSite());
		assertEquals( 10, urlSeenCounter.getCount());
		assertEquals( 1001, urlSeenCounter.getMonitoredSince());
		
		urlSeenCounter = new URLSeenCounter( "example3.co.uk", "http://www.example3.co.uk/path/to/url.html?xxx", 13, 2002);
		assertEquals( "example3.co.uk", urlSeenCounter.getSite());
		assertEquals( 13, urlSeenCounter.getCount());
		assertEquals( 2002, urlSeenCounter.getMonitoredSince());

	}

}
