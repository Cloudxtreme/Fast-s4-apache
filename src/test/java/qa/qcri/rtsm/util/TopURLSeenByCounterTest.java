package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.item.URLSeenCounter;

public class TopURLSeenByCounterTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		TopURLSeenByCounter queue = new TopURLSeenByCounter(3);

		queue.add(new URLSeenCounter("site1", "url1", 10, 1));
		assertEquals(queue.size(), 1);

		queue.add(new URLSeenCounter("site2", "url2", 20, 2));
		assertEquals(queue.size(), 2);

		queue.add(new URLSeenCounter("site3", "url3", 30, 3));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url3");
		
		queue.add(new URLSeenCounter("site4", "url4", 40, 4));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url4");
		
		assertTrue( queue.containsURL("url4") );
		assertTrue( queue.containsURL("url3") );
		assertTrue( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );
		
		queue.add(new URLSeenCounter("site4", "url4", 10, 4));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url3");
		
		assertTrue( queue.containsURL("url4") );
		assertTrue( queue.containsURL("url3") );
		assertTrue( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );
		
		queue.add(new URLSeenCounter("site5", "url5", 70, 5));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url5");
		
		assertTrue( queue.containsURL("url5") );
		assertFalse( queue.containsURL("url4") );
		assertTrue( queue.containsURL("url3") );
		assertTrue( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );
		
		queue.add(new URLSeenCounter("site6", "url6", 60, 6));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url5");
		
		assertTrue( queue.containsURL("url6") );
		assertTrue( queue.containsURL("url5") );
		assertFalse( queue.containsURL("url4") );
		assertTrue( queue.containsURL("url3") );
		assertFalse( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );
		
		queue.add(new URLSeenCounter("site7", "url7", 50, 7));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url5");
		
		assertTrue( queue.containsURL("url7") );
		assertTrue( queue.containsURL("url6") );
		assertTrue( queue.containsURL("url5") );
		assertFalse( queue.containsURL("url4") );
		assertFalse( queue.containsURL("url3") );
		assertFalse( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );
		
		queue.add(new URLSeenCounter("site5", "url5", 10, 5));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url6");
		
		assertTrue( queue.containsURL("url7") );
		assertTrue( queue.containsURL("url6") );
		assertTrue( queue.containsURL("url5") );
		assertFalse( queue.containsURL("url4") );
		assertFalse( queue.containsURL("url3") );
		assertFalse( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );
		
		queue.add(new URLSeenCounter("site4", "url4", 40, 4));
		assertEquals(queue.size(), 3);
		assertEquals(queue.peek().getUrl(), "url6");
		
		assertTrue( queue.containsURL("url7") );
		assertTrue( queue.containsURL("url6") );
		assertFalse( queue.containsURL("url5") );
		assertTrue( queue.containsURL("url4") );
		assertFalse( queue.containsURL("url3") );
		assertFalse( queue.containsURL("url2") );
		assertFalse( queue.containsURL("url1") );

	}
}
