package qa.qcri.rtsm.persist.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;

import org.junit.Before;
import org.junit.Test;

public class CassandraSchemasTest {

	final static String TEST_KEYSPACE = "TestKeyspace7";

	public static void clearKeyspace(String keyspaceName) {
		// Start with empty keyspace
		Cluster cluster = CassandraSchema.getCluster();
		try {
			cluster.dropKeyspace(keyspaceName, true);
			
			// If this is not null, the keyspace was not deleted
			if( cluster.describeKeyspace(keyspaceName) != null ) {
				throw new IllegalStateException("The key space was not deleted");
			}

		} catch (HInvalidRequestException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetCluster() {
		Cluster cluster = CassandraSchema.getCluster();
		assertTrue(cluster != null);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testGetKeyspaceString() {
		clearKeyspace(TEST_KEYSPACE);
		
		CassandraSchema cassandraSchema = new CassandraSchema(TEST_KEYSPACE);
		Keyspace keyspace = cassandraSchema.getKeyspace();
		assertTrue(keyspace != null);

		CassandraPersistentTimeSeries c = new CassandraPersistentTimeSeries(TEST_KEYSPACE, CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
		c.set("testPart", "testKey", new Long(10), new Integer(20));
		c.set("testPart", "testKey", new Long(30), new Integer(42));

		// Open the keyspace again and check the data are still there
		keyspace = cassandraSchema.getKeyspace();
		assertEquals( new Integer(20), c.get("testPart", "testKey", new Long(10) ) );
		assertEquals( new Integer(42), c.get("testPart", "testKey", new Long(30) ) );	
		assertEquals( null, c.get("testPart", "testKey", new Long(20) ) );
	}
}
