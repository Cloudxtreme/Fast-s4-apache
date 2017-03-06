package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.util.IntervalCounter.AlreadyFlushedException;

public class IntervalCounterTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testIntervalCounter() {
		IntervalCounter c1 = new IntervalCounter(10l);
		assertTrue(c1 != null);

		IntervalCounter c2 = new IntervalCounter(IntervalCounter.ONE_SECOND);
		assertTrue(c2 != null);
	}

	@Test
	public void testIncrementCounterLong() throws AlreadyFlushedException {
		IntervalCounter c1 = new IntervalCounter(10l);
		c1.incrementCounter(4);
		c1.incrementCounter(5);
		c1.incrementCounter(6);
		c1.incrementCounter(14);
		c1.incrementCounter(15);
		c1.incrementCounter(21);

		assertEquals(3, c1.getCounter(0l));
		assertEquals(2, c1.getCounter(10l));
		assertEquals(1, c1.getCounter(20l));
	}

	@Test
	public void testIncrementCounterLongInt() throws AlreadyFlushedException {
		IntervalCounter c1 = new IntervalCounter(10l);
		c1.incrementCounter(1);
		c1.incrementCounter(0);
		c1.incrementCounter(3);
		c1.incrementCounter(13, 5);
		c1.incrementCounter(16, 25);
		c1.incrementCounter(20, 101);

		assertEquals(3, c1.getCounter(0l));
		assertEquals(30, c1.getCounter(10l));
		assertEquals(101, c1.getCounter(20l));
	}
	
	@Test
	public void testSetCounterLongInt() throws AlreadyFlushedException {
		IntervalCounter c1 = new IntervalCounter(10l);
		c1.setCounter(13, 5);
		c1.setCounter(16, 25);
		c1.setCounter(20, 101);
		c1.setCounter(24, 301);
		c1.setCounter(13, 4);

		assertEquals(0, c1.getCounter(0l));
		assertEquals(4, c1.getCounter(10l));
		assertEquals(301, c1.getCounter(20l));
	}

	@Test
	public void testStartOfInterval() {
		IntervalCounter c1 = new IntervalCounter(10l);
		assertEquals(0, c1.startOfInterval(3));
		assertEquals(30, c1.startOfInterval(31));
		assertEquals(1290, c1.startOfInterval(1291));
	}

	@Test
	public void testFlush() throws AlreadyFlushedException {
		IntervalCounter c1 = new IntervalCounter(10l);
		c1.incrementCounter(1);
		c1.incrementCounter(0, 9);
		c1.incrementCounter(3);
		c1.incrementCounter(13, 8);
		c1.incrementCounter(16, 25);
		c1.incrementCounter(20, 101);
		c1.incrementCounter(23, 4);
		c1.incrementCounter(39, 113);
		c1.incrementCounter(79, 19);

		TreeMap<Long, Integer> oldC1 = c1.flush(25);
		assertEquals(2, oldC1.keySet().size());
		assertEquals(new Integer(11), oldC1.get(new Long(0)));
		assertEquals(new Integer(33), oldC1.get(new Long(10)));

		try {
			c1.incrementCounter(19, 1);
			fail("Should have thrown an exception");
		} catch (AlreadyFlushedException e) {

		}
		try {
			c1.incrementCounter(0);
			fail("Should have thrown an exception");
		} catch (AlreadyFlushedException e) {

		}
		c1.incrementCounter(20, 1);

		TreeMap<Long, Integer> oldC2 = c1.flush(41);
		assertEquals(oldC2.keySet().size(), 2);
		assertEquals(new Integer(106), oldC2.get(new Long(20)));
		assertEquals(new Integer(113), oldC2.get(new Long(30)));
	}
	
	@Test
	public void testGetNumIntervals() throws AlreadyFlushedException {
		IntervalCounter c1 = new IntervalCounter(10l);
		c1.incrementCounter(1);
		c1.incrementCounter(0, 9);
		c1.incrementCounter(3);
		assertEquals( 1, c1.getNumIntervals());
		c1.incrementCounter(13, 8);
		c1.incrementCounter(16, 25);
		assertEquals( 2, c1.getNumIntervals());
		c1.incrementCounter(20, 101);
		c1.incrementCounter(23, 4);
		assertEquals( 3, c1.getNumIntervals());
		c1.incrementCounter(39, 113);
		assertEquals( 4, c1.getNumIntervals());
		c1.incrementCounter(79, 19);
		assertEquals( 5, c1.getNumIntervals());
		
		TreeMap<Long, Integer> oldC1 = c1.flush(25);
		assertEquals(2, oldC1.keySet().size());
		assertEquals(new Integer(11), oldC1.get(new Long(0)));
		assertEquals(new Integer(33), oldC1.get(new Long(10)));
		
		assertEquals(3, c1.getNumIntervals());
		
		try {
			c1.incrementCounter(19, 1);
			fail("Should have thrown an exception");
		} catch (AlreadyFlushedException e) {

		}
		assertEquals(3, c1.getNumIntervals());
		try {
			c1.incrementCounter(0);
			fail("Should have thrown an exception");
		} catch (AlreadyFlushedException e) {

		}
		c1.incrementCounter(20, 1);
		assertEquals(3, c1.getNumIntervals());

		TreeMap<Long, Integer> oldC2 = c1.flush(41);
		assertEquals(oldC2.keySet().size(), 2);
		assertEquals(new Integer(106), oldC2.get(new Long(20)));
		assertEquals(new Integer(113), oldC2.get(new Long(30)));
		
		assertEquals(1, c1.getNumIntervals());
		
		c1.flush(10000);
		assertEquals(0, c1.getNumIntervals());
	}
	
	@Test
	public void testGetSumValues() throws AlreadyFlushedException {
		IntervalCounter c1 = new IntervalCounter(10l);
		c1.incrementCounter(1);
		assertEquals(1, c1.getSumValues());
		c1.incrementCounter(0, 9);
		assertEquals(1+9, c1.getSumValues());
		c1.incrementCounter(3);
		assertEquals(1+9+1, c1.getSumValues());
		c1.incrementCounter(13, 8);
		assertEquals(1+9+1+8, c1.getSumValues());
		c1.incrementCounter(16, 25);
		assertEquals(1+9+1+8+25, c1.getSumValues());
		c1.incrementCounter(20, 101);
		assertEquals(1+9+1+8+25+101, c1.getSumValues());
		c1.incrementCounter(23, 4);
		assertEquals(1+9+1+8+25+101+4, c1.getSumValues());
		c1.incrementCounter(39, 113);
		assertEquals(1+9+1+8+25+101+4+113, c1.getSumValues());
		c1.incrementCounter(79, 19);
		assertEquals(1+9+1+8+25+101+4+113+19, c1.getSumValues());

		TreeMap<Long, Integer> oldC1 = c1.flush(25);
		assertEquals(2, oldC1.keySet().size());
		assertEquals(new Integer(11), oldC1.get(new Long(0)));
		assertEquals(new Integer(33), oldC1.get(new Long(10)));
		
		assertEquals((1+9+1+8+25+101+4+113+19)-(11+33), c1.getSumValues());

		try {
			c1.incrementCounter(19, 1);
			fail("Should have thrown an exception");
		} catch (AlreadyFlushedException e) {

		}
		try {
			c1.incrementCounter(0);
			fail("Should have thrown an exception");
		} catch (AlreadyFlushedException e) {

		}
		c1.incrementCounter(20, 1);
		assertEquals((1+9+1+8+25+101+4+113+19)-(11+33)+(1), c1.getSumValues());

		TreeMap<Long, Integer> oldC2 = c1.flush(41);
		assertEquals(oldC2.keySet().size(), 2);
		assertEquals(new Integer(106), oldC2.get(new Long(20)));
		assertEquals(new Integer(113), oldC2.get(new Long(30)));
		assertEquals((1+9+1+8+25+101+4+113+19)-(11+33)+(1)-(106+113), c1.getSumValues());
		
		c1.flush(10000);
		assertEquals(0, c1.getSumValues());
	}

}
