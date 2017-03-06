package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.util.TopURLSeenByCounterWithLeastRecentlyUsed.WorkerFactory;

public class TopURLSeenByCounterWithLeastRecentlyUsedTest {

	static class DummyWorker implements LastWorkedTime {
		String key;
		long lastWorked;

		public DummyWorker(String key) {
			this.key = key;
			this.lastWorked = -1;
		}

		public boolean hasWorked() {
			return (lastWorked >= 0);
		}

		public long getLastWorkedTime() {
			return lastWorked;
		}

		public void work() throws InterruptedException {
			long now = (new Date()).getTime();
			lastWorked = now;
			Thread.sleep(20);
		}
		
		public String getKey() {
			return key;
		}
	}

	@Before
	public void setUp() throws Exception {
	}

	private TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker> getInstance() {
		return new TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker>(3, new WorkerFactory<DummyWorker>() {
			@Override
			public DummyWorker newInstance(String url) {
				return new DummyWorker(url);
			}
		});
	}

	@Test
	public void testTopURLSeenByCounterWithRoundRobin() {
		TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker> top = getInstance();
		assertTrue(top != null);
		assertEquals(null, top.getNext());
	}

	@Test
	public void testAddGetNext1() throws InterruptedException {
		TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker> top = getInstance();
		top.add( new URLSeenCounter("sa", "a", 10, 1));
		assertEquals( 1, top.size() );
		DummyWorker a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sb", "b", 15, 2));
		assertEquals( 2, top.size() );
		DummyWorker b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		top.add( new URLSeenCounter("sc", "c", 20, 3));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		
		top.add( new URLSeenCounter("sd", "d", 30, 4));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		
		top.add( new URLSeenCounter("se", "e", 5, 5));
		
		assertEquals( 3, top.size() );
		assertFalse( top.containsURL("a") );
		assertTrue( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		assertFalse( top.containsURL("e") );
	
		DummyWorker d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		DummyWorker c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
				
		b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
	}

	@Test
	public void testAddGetNext2() throws InterruptedException {
		TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker> top = getInstance();
		top.add( new URLSeenCounter("sa", "a", 10, 1));
		assertEquals( 1, top.size() );
		DummyWorker a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sb", "b", 15, 2));
		assertEquals( 2, top.size() );
		DummyWorker b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sa", "a", 100, 1));
		
		b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		top.add( new URLSeenCounter("sc", "c", 20, 3));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		
		top.add( new URLSeenCounter("sd", "d", 30, 4));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		
		top.add( new URLSeenCounter("se", "e", 5, 5));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertFalse( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		assertFalse( top.containsURL("e") );
	
		DummyWorker d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		DummyWorker c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
				
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
	}

	@Test
	public void testAddGetNext3() throws InterruptedException {
		TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker> top = getInstance();
		top.add( new URLSeenCounter("sa", "a", 10, 1));
		assertEquals( 1, top.size() );
		DummyWorker a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sb", "b", 15, 2));
		assertEquals( 2, top.size() );
		DummyWorker b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sb", "b", 5, 2));
		
		b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		top.add( new URLSeenCounter("sc", "c", 20, 3));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		
		top.add( new URLSeenCounter("sd", "d", 30, 4));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		
		top.add( new URLSeenCounter("se", "e", 5, 5));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertFalse( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		assertFalse( top.containsURL("e") );
	
		DummyWorker d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		DummyWorker c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
				
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
	}
	
	@Test
	public void testAddGetNext4() throws InterruptedException {
		TopURLSeenByCounterWithLeastRecentlyUsed<DummyWorker> top = getInstance();
		top.add( new URLSeenCounter("sa", "a", 10, 1));
		assertEquals( 1, top.size() );
		DummyWorker a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sb", "b", 15, 2));
		assertEquals( 2, top.size() );
		DummyWorker b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("sb", "b", 5, 2));
		
		b = top.getNext();
		assertEquals("b", b.getKey());
		b.work();
		
		top.add( new URLSeenCounter("sc", "c", 20, 3));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		
		top.add( new URLSeenCounter("sd", "d", 11, 4));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		
		top.add( new URLSeenCounter("se", "e", 5, 5));
		
		assertEquals( 3, top.size() );
		assertTrue( top.containsURL("a") );
		assertFalse( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		assertFalse( top.containsURL("e") );
	
		DummyWorker c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		DummyWorker d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
		
		a = top.getNext();
		assertEquals("a", a.getKey());
		a.work();
		
		top.add( new URLSeenCounter("se", "e", 12, 5) );
		assertEquals( 3, top.size() );
		
		assertFalse( top.containsURL("a") );
		assertFalse( top.containsURL("b") );
		assertTrue( top.containsURL("c") );
		assertTrue( top.containsURL("d") );
		assertTrue( top.containsURL("e") );
		
		DummyWorker e = top.getNext();
		assertEquals("e", e.getKey());
		e.work();
		
		c = top.getNext();
		assertEquals("c", c.getKey());
		c.work();
		
		d = top.getNext();
		assertEquals("d", d.getKey());
		d.work();
	}


}
