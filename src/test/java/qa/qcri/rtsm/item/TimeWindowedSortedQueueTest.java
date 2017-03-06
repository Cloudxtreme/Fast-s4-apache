package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

public class TimeWindowedSortedQueueTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testTimeWindowedQueue() {
		TimeWindowedSortedQueue<String> queue = new TimeWindowedSortedQueue<String>(1000);
		assertTrue(queue != null);
	}

	@Test
	public void testInsertElementLongT() {
		TimeWindowedSortedQueue<String> queue = new TimeWindowedSortedQueue<String>(1000);
		assertTrue(queue != null);

		queue.insertElement(1000, "x1000");
		queue.insertElement(1200, "x1200");
		assertEquals(2, queue.size());
		queue.insertElement(1400, "x1400");
		queue.insertElement(2000, "x2000");
		assertEquals(4, queue.size());

		queue.insertElement(2000, "x2000");
		assertEquals(4, queue.size());
	}

	@Test
	public void testFlushOldElementsLong1() {
		TimeWindowedSortedQueue<String> queue = new TimeWindowedSortedQueue<String>(1000);
		assertTrue(queue != null);

		queue.insertElement(1200, "x1200");
		queue.insertElement(2000, "x2000");
		queue.insertElement(1000, "x1000");
		queue.insertElement(1400, "x1400");

		Vector<String> result = queue.flushOldElements(2500);
		assertEquals(3, result.size());
		assertEquals(1, queue.size());

		assertEquals("x1000", result.get(0));
		assertEquals("x1200", result.get(1));
		assertEquals("x1400", result.get(2));

		result = queue.flushOldElements(3100);
		assertEquals(1, result.size());
		assertEquals(0, queue.size());

		assertEquals("x2000", result.get(0));
		
		result = queue.flushOldElements();
		assertEquals(0, result.size());
	}
	
	@Test
	public void testFlushOldElementsLong2() {
		TimeWindowedSortedQueue<String> queue = new TimeWindowedSortedQueue<String>(1);
		assertTrue(queue != null);
		
		queue.insertElement(23, "x23");
		queue.insertElement(38, "x38");
		queue.insertElement(1, "x01");
		queue.insertElement(50, "x50");
		queue.insertElement(8, "x08");
		queue.insertElement(27, "x27");
		queue.insertElement(15, "x15");
		queue.insertElement(25, "x25");
		queue.insertElement(45, "x45");
		queue.insertElement(18, "x18");
		queue.insertElement(31, "x31");
		queue.insertElement(28, "x28");
		queue.insertElement(44, "x44");
		queue.insertElement(20, "x20");
		queue.insertElement(22, "x22");
		queue.insertElement(48, "x48");
		queue.insertElement(17, "x17");
		queue.insertElement(24, "x24");
		queue.insertElement(11, "x11");
		queue.insertElement(37, "x37");
		queue.insertElement(19, "x19");
		queue.insertElement(5, "x05");
		queue.insertElement(49, "x49");
		queue.insertElement(43, "x43");
		queue.insertElement(42, "x42");
		queue.insertElement(13, "x13");
		queue.insertElement(40, "x40");
		queue.insertElement(32, "x32");
		queue.insertElement(33, "x33");
		queue.insertElement(10, "x10");
		queue.insertElement(46, "x46");
		queue.insertElement(3, "x03");
		queue.insertElement(47, "x47");
		queue.insertElement(14, "x14");
		queue.insertElement(16, "x16");
		queue.insertElement(7, "x07");
		queue.insertElement(41, "x41");
		queue.insertElement(6, "x06");
		queue.insertElement(30, "x30");
		queue.insertElement(35, "x35");
		queue.insertElement(34, "x34");
		queue.insertElement(2, "x02");
		queue.insertElement(29, "x29");
		queue.insertElement(4, "x04");
		queue.insertElement(21, "x21");
		queue.insertElement(26, "x26");
		queue.insertElement(9, "x09");
		queue.insertElement(39, "x39");
		queue.insertElement(36, "x36");
		queue.insertElement(12, "x12");
		
		assertEquals(50, queue.size());
		
		String lastItem = "x00";
		for( String item: queue.flushOldElements(26) ) {
			assertTrue(lastItem.compareTo(item) < 0);
			lastItem = item;
		}
		assertEquals(25, queue.size());
		for( String item: queue.flushOldElements(51) ) {
			assertTrue(lastItem.compareTo(item) < 0);
			lastItem = item;
		}
		assertEquals(0, queue.size());
	}
}
