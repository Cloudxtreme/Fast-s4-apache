package qa.qcri.rtsm.persist.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

public class CassandraPersistentContentTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testCassandraPersistentContent() {
		CassandraPersistentContent cc = new CassandraPersistentContent(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_CONTENT);
		assertTrue(cc != null);
	}

	@Test
	public void testSetGet() {
		CassandraPersistentContent cc = new CassandraPersistentContent(CassandraSchemasTest.TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_CONTENT);
		assertTrue(cc != null);

		try {
			cc.set("url1", "k1", "v10");
			fail("Should have thrown an exception");
		} catch (IllegalArgumentException e) {

		}

		cc.set("url1", "title", "Test title");
		assertEquals("Test title", cc.get("url1", "title"));

		try {
			assertEquals("v10", cc.get("url1", "k1"));
			fail("Should have thrown an exception");
		} catch (IllegalArgumentException e) {

		}
	}
}
