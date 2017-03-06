package qa.qcri.rtsm.persist.cassandra;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import qa.qcri.rtsm.util.RTSMConf;
import qa.qcri.rtsm.util.Util;

public class CassandraSchema {

	private static String CLUSTER_NAME = (new RTSMConf()).getAppName() + "_Cluster";

	private static String DEFAULT_KEYSPACE_NAME = (new RTSMConf()).getAppName() + "_Keyspace3";

	public static final String COLUMNFAMILY_NAME_TIMESERIES_VISITS = "TimeSeriesVisit";

	public static final String COLUMNFAMILY_NAME_TIMESERIES_SOURCES = "TimeSeriesSource";
	
	public static final String COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK = "TimeSeriesFacebook";
	
	public static final String COLUMNFAMILY_NAME_TWEETS = "Tweet";
	
	public static final String COLUMNFAMILY_NAME_CONTENT = "Content";
	
	public static final String COLUMNFAMILY_NAME_URL_FB_ID = "URL_FACEBOOKId";

	final Cluster cluster;

	final Keyspace keyspace;

	public CassandraSchema() {
		this(DEFAULT_KEYSPACE_NAME);
	}

	public CassandraSchema(String keyspaceName) {
		cluster = getCluster();
		if (cluster != null) {
			Util.logDebug(this, "Using cluster '" + cluster.getName() + "' in '" + RTSMConf.CASSANDRA_HOST + "' port " + RTSMConf.CASSANDRA_PORT );
		} else {
			throw new IllegalStateException("Could not open cluster in '" + RTSMConf.CASSANDRA_HOST + "' port " + RTSMConf.CASSANDRA_PORT );
		}

		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);

		// If the keyspace does not exist, create it
		if (keyspaceDef == null) {
			Util.logDebug(this, "Keyspace '" + keyspaceName + "' does not exist, creating");
			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName);
			cluster.addKeyspace(newKeyspace, true);
		} else {
			Util.logDebug(this, "Using keyspace '" + keyspaceName + "' keyspace");
		}

		// Load keyspace
		keyspace = HFactory.createKeyspace(keyspaceName, cluster);

		// Add column families as needed
		if (!hasColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_VISITS)) {
			Util.logDebug(this, "Will try to create column family " + COLUMNFAMILY_NAME_TIMESERIES_VISITS);
			createTimeSeriesColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_VISITS);
			if (!hasColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_VISITS)) {
				Util.logWarning(this, "Failed to create column family " + COLUMNFAMILY_NAME_TIMESERIES_VISITS);
				throw new IllegalStateException("Could not create " + COLUMNFAMILY_NAME_TIMESERIES_VISITS);
			}
		}
		
		if (!hasColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_SOURCES)) {
			Util.logDebug(this, "Will try to create column family " + COLUMNFAMILY_NAME_TIMESERIES_SOURCES);
			createTimeSeriesColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_SOURCES);
			if (!hasColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_SOURCES)) {
				Util.logWarning(this, "Failed to create column family " + COLUMNFAMILY_NAME_TIMESERIES_SOURCES);
				throw new IllegalStateException("Could not create " + COLUMNFAMILY_NAME_TIMESERIES_SOURCES);
			}
		}
		
		if (!hasColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK)) {
			Util.logDebug(this, "Will try to create column family " + COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK);
			createTimeSeriesColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK);
			if (!hasColumnFamily(COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK)) {
				Util.logWarning(this, "Failed to create column family " + COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK);
				throw new IllegalStateException("Could not create " + COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK);
			}
		}
		
		if (!hasColumnFamily(COLUMNFAMILY_NAME_TWEETS)) {
			Util.logDebug(this, "Will try to create column family " + COLUMNFAMILY_NAME_TWEETS);
			createTweetsColumnFamily(COLUMNFAMILY_NAME_TWEETS);
			if (!hasColumnFamily(COLUMNFAMILY_NAME_TWEETS)) {
				Util.logWarning(this, "Failed to create column family " + COLUMNFAMILY_NAME_TWEETS);
				throw new IllegalStateException("Could not create " + COLUMNFAMILY_NAME_TWEETS);
			}
		}
		
		if (!hasColumnFamily(COLUMNFAMILY_NAME_CONTENT)) {
			Util.logDebug(this, "Will try to create column family " + COLUMNFAMILY_NAME_CONTENT);
			createContentColumnFamily(COLUMNFAMILY_NAME_CONTENT);
			if (!hasColumnFamily(COLUMNFAMILY_NAME_CONTENT)) {
				Util.logWarning(this, "Failed to create column family " + COLUMNFAMILY_NAME_CONTENT);
				throw new IllegalStateException("Could not create " + COLUMNFAMILY_NAME_CONTENT);
			}
		}

		if (!hasColumnFamily(COLUMNFAMILY_NAME_URL_FB_ID)) {
			System.out.println("Will try to create column family " + COLUMNFAMILY_NAME_URL_FB_ID);
			createColumnUrlIdFamily(COLUMNFAMILY_NAME_URL_FB_ID);
			if (!hasColumnFamily(COLUMNFAMILY_NAME_URL_FB_ID)) {
				System.out.println("Failed to create column family " + COLUMNFAMILY_NAME_URL_FB_ID);
				throw new IllegalStateException("Could not create " + COLUMNFAMILY_NAME_URL_FB_ID);
			}
		}
		Util.logDebug(this, "Cassandra schemas initialized/tested correctly");
	}

	public static Cluster getCluster() {
		CassandraHostConfigurator config = new CassandraHostConfigurator();
        config.setHosts(RTSMConf.CASSANDRA_HOST);
        config.setPort(RTSMConf.CASSANDRA_PORT);
		return HFactory.getOrCreateCluster(CLUSTER_NAME, config, new ConcurrentHashMap<String, String>());
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}

	private boolean hasColumnFamily(String columnFamilyName) {
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace.getKeyspaceName());

		List<ColumnFamilyDefinition> columns = keyspaceDef.getCfDefs();
		for (ColumnFamilyDefinition cfDef : columns) {
			if (cfDef.getName().equals(columnFamilyName)) {
				return true;
			}
		}
		return false;
	}

	
	
	
	private void createColumnUrlIdFamily(String columnFamilyName) {
		// Create a column
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamilyName, ComparatorType.UTF8TYPE);

		// The key is a URL (UTF8)
		cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());

		// The values are strings
		cfDef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

		String rc = cluster.addColumnFamily(cfDef, true);

		System.out.println("Result of adding '" + cfDef.getName() + "' to '" + keyspace.getKeyspaceName() + "': " + rc);
	}
	
	private void createTimeSeriesColumnFamily(String columnFamilyName) {
		// Create a super-column, sub-key is a string
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamilyName, ComparatorType.UTF8TYPE);

		// This is a super-column
		cfDef.setColumnType(ColumnType.SUPER);

		// The key is a URL (UTF8)
		cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());

		// The sub-key is a Long
		cfDef.setSubComparatorType(ComparatorType.LONGTYPE);

		// The values are counters
		cfDef.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());

		String rc = cluster.addColumnFamily(cfDef, true);

		Util.logDebug(this, "Result of adding '" + cfDef.getName() + "' to '" + keyspace.getKeyspaceName() + "': " + rc);
	}

	private void createTweetsColumnFamily(String columnFamilyName) {
		// Create a column
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamilyName, ComparatorType.LONGTYPE);

		// The key is a URL (UTF8)
		cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());

		// The values are strings
		cfDef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

		String rc = cluster.addColumnFamily(cfDef, true);

		Util.logDebug(this, "Result of adding '" + cfDef.getName() + "' to '" + keyspace.getKeyspaceName() + "': " + rc);
	}
	
	private void createContentColumnFamily(String columnFamilyName) {
		// Create a column
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamilyName, ComparatorType.UTF8TYPE);

		// The key is a URL (UTF8)
		cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());

		// The values are strings
		cfDef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

		String rc = cluster.addColumnFamily(cfDef, true);

		Util.logDebug(this, "Result of adding '" + cfDef.getName() + "' to '" + keyspace.getKeyspaceName() + "': " + rc);
	}
	
	
	
	public static boolean isValidColumnFamilyNameContent(String columnFamilyName) {
		return columnFamilyName.equals(COLUMNFAMILY_NAME_CONTENT);
	}

	public static boolean isValidColumnFamilyNameTweets(String columnFamilyName) {
		return columnFamilyName.equals(COLUMNFAMILY_NAME_TWEETS);
	}

	public static boolean isValidColumnFamilyNameTimeSeries(String columnFamilyName) {
		return (columnFamilyName.equals(COLUMNFAMILY_NAME_TIMESERIES_VISITS) || columnFamilyName.equals(COLUMNFAMILY_NAME_TIMESERIES_SOURCES) || columnFamilyName.equals(COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK));
	}
	public static boolean isValidColumnFamilyUrlId(String columnFamilyName) {
		return columnFamilyName.equals(COLUMNFAMILY_NAME_URL_FB_ID);
	}
}