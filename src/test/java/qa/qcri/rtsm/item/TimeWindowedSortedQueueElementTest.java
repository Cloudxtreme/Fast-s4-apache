package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.item.TimeWindowedSortedQueue.Element;

public class TimeWindowedSortedQueueElementTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testElement() {
		Element<String> element = new Element<String>(1, "e1");
		assertTrue(element != null);
	}

	@Test
	public void testCompareTo() {
		Element<String> e1 = new Element<String>(1, "e1");
		Element<String> e2 = new Element<String>(2, "e2");
		Element<String> e3 = new Element<String>(2, "e3");
		
		assertEquals(-1, e1.compareTo(e2));
		assertEquals(1, e2.compareTo(e1));
		assertEquals(-1, e1.compareTo(e3));
		assertEquals(1, e3.compareTo(e1));
		assertEquals(0, e2.compareTo(e3));
		assertEquals(0, e3.compareTo(e2));
	}

	@Test
	public void testEqualsObject() {
		Element<String> e1 = new Element<String>(1, "e1");
		Element<String> e2 = new Element<String>(2, "e2");
		Element<String> e3 = new Element<String>(3, "e2");
		
		assertTrue(e1.equals(e1));
		assertTrue(e2.equals(e3));
		assertFalse(e1.equals(e3));
	}

	@Test
	public void testGetTimestamp() {
		Element<String> element = new Element<String>(42, "e1");
		assertEquals(42, element.getTimestamp());
	}

	@Test
	public void testGetItem() {
		Element<String> element = new Element<String>(42, "e1");
		assertEquals("e1", element.getItem());
	}

}
