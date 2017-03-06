package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class VisitTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetStrippedNormalizedURL() {
		Visit v = new Visit();
		v.setUrl("http://www.example.com/with?query");
		assertEquals("http://www.example.com/with", v.getStrippedNormalizedUrl());
		v.setUrl("http://www.example.com/with?query#fragment");
		assertEquals("http://www.example.com/with", v.getStrippedNormalizedUrl());
		v.setUrl("http://www.example.com/with#fragment");
		assertEquals("http://www.example.com/with", v.getStrippedNormalizedUrl());
	}
	
	@Test
	public void testGetSetSource() {
		Visit v = new Visit();
		v.setSource("test-source");
		assertEquals("test-source", v.getSource());
	}

	@Test
	public void testGetSetSearchTerms() {
		Visit v = new Visit();
		v.setSearchTerms("test-terms");
		assertEquals("test-terms", v.getSearchTerms());
	}
	
	@Test
	public void testGetSetReferral() {
		Visit v = new Visit();
		v.setReferral("test-ref");
		assertEquals("test-ref", v.getReferral());
	}

	@Test
	public void testGetSetVisitorID() {
		Visit v = new Visit();
		v.setReferral("test-ref");
		assertEquals("test-ref", v.getReferral());
	}

	@Test
	public void testGetSetUrl() {
		Visit v = new Visit();
		v.setUrl("http://www.example.com/with?query");
		assertEquals("http://www.example.com/with?query", v.getUrl());
		v.setUrl("http://www.example.com/with?query#fragment");
		assertEquals("http://www.example.com/with?query#fragment", v.getUrl());
		v.setUrl("http://www.example.com/with#fragment");
		assertEquals("http://www.example.com/with#fragment", v.getUrl());
	}

	@Test
	public void testGetSetSampleRate() {
		Visit v = new Visit();
		v.setSampleRate(0.42);
		assertEquals(0.42, v.getSampleRate(), 1e-5);
	}

	@Test
	public void testGetSetTimestamp() {
		Visit v = new Visit();
		v.setTimestamp(43l);
		assertEquals(43l, v.getTimestamp());
	}


}
