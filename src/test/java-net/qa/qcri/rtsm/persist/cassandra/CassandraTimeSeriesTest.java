package qa.qcri.rtsm.persist.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.util.IntervalCounter;

public class CassandraTimeSeriesTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testCassandraTimeSeriesString() {
		CassandraPersistentTimeSeries cts = new CassandraPersistentTimeSeries(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
		assertTrue(cts != null);
	}

	/**
	 * Test the deprecated method that sets values one by one.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void testSetGet1() {
		CassandraSchemasTest.clearKeyspace(CassandraSchemasTest.TEST_KEYSPACE);
		CassandraPersistentTimeSeries cts = new CassandraPersistentTimeSeries(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
		assertTrue(cts != null);

		cts.set(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", new Long(10), new Integer(42));
		cts.set(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", new Long(20), new Integer(43));
		cts.set(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", new Long(30), new Integer(44));
		cts.set(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", new Long(40), new Integer(45));

		cts.set(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", new Long(30), new Integer(107));
		cts.set(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", new Long(40), new Integer(108));

		Integer count1 = cts.get(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", new Long(30));
		assertEquals(new Integer(107), count1);

		Integer count2 = cts.get(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", new Long(20));
		assertEquals(new Integer(43), count2);

		Integer count3 = cts.get(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", new Long(90));
		assertEquals(null, count3);
	}

	@Test
	public void testSetGet2() {
		CassandraSchemasTest.clearKeyspace(CassandraSchemasTest.TEST_KEYSPACE);
		CassandraPersistentTimeSeries cts = new CassandraPersistentTimeSeries(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
		assertTrue(cts != null);

		TreeMap<Long, Integer> counters = new TreeMap<Long, Integer>();
		counters.put(new Long(10), new Integer(42));
		counters.put(new Long(20), new Integer(43));
		counters.put(new Long(30), new Integer(44));
		counters.put(new Long(40), new Integer(45));
		cts.set(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", counters);

		counters.clear();

		counters.put(new Long(30), new Integer(107));
		counters.put(new Long(40), new Integer(108));

		cts.set(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", counters);

		Integer count1 = cts.get(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", new Long(30));
		assertEquals(new Integer(107), count1);

		Integer count2 = cts.get(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", new Long(20));
		assertEquals(new Integer(43), count2);

		Integer count3 = cts.get(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", new Long(90));
		assertEquals(null, count3);
	}

	@Test
	public void testGetPartKey() {
		CassandraSchemasTest.clearKeyspace(CassandraSchemasTest.TEST_KEYSPACE);
		CassandraPersistentTimeSeries cts = new CassandraPersistentTimeSeries(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
		assertTrue(cts != null);

		TreeMap<Long, Integer> counters1 = new TreeMap<Long, Integer>();
		counters1.put(new Long(10), new Integer(42));
		counters1.put(new Long(20), new Integer(43));
		counters1.put(new Long(30), new Integer(44));
		counters1.put(new Long(40), new Integer(45));
		cts.set(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test", counters1);
		
		TreeMap<Long, Integer> counters2 = new TreeMap<Long, Integer>();
		counters2.put(new Long(101), new Integer(4));
		counters2.put(new Long(203), new Integer(3));
		counters2.put(new Long(306), new Integer(9));
		cts.set(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test", counters2);
		
		TreeMap<Long, Integer> response1 = cts.get(IntervalCounter.STR_TEN_SECONDS, "http://www.example.com/#test");
		assertEquals(4, response1.size());
		assertEquals(new Integer(42), response1.get(new Long(10)));
		assertEquals(new Integer(43), response1.get(new Long(20)));
		assertEquals(new Integer(44), response1.get(new Long(30)));
		assertEquals(new Integer(45), response1.get(new Long(40)));
		
		TreeMap<Long, Integer> response2 = cts.get(IntervalCounter.STR_ONE_MINUTE, "http://www.example.com/#test");
		assertEquals(3, response2.size());
		assertEquals(new Integer(4), response2.get(new Long(101)));
		assertEquals(new Integer(3), response2.get(new Long(203)));
		assertEquals(new Integer(9), response2.get(new Long(306)));
		
				
		
	}
}
