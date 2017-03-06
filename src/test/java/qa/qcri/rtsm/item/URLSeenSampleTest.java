package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class URLSeenSampleTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testURLSeenSampleStringFloat() {
		URLSeenSample urlSeen1 = new URLSeenSample("www.example.com", "http://www.example.com/url.html", 0.1);
		assertEquals( urlSeen1.getSite(), "www.example.com" );
		assertEquals( urlSeen1.getSampleRate(), 0.1, 1e-8 );
		
		URLSeenSample urlSeen2 = new URLSeenSample("example2.co.uk", "https://www.example2.co.uk/path/to/url.html?xxx", 0.2);
		assertEquals( urlSeen2.getSite(), "example2.co.uk" );
		assertEquals( urlSeen2.getSampleRate(), 0.2, 1e-8 );
	}

}
