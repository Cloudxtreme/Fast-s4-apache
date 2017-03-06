package qa.qcri.rtsm.persist.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class CassandraPersistentContent {
	
	public final static String KEY_TITLE = "title";

	public final static String KEY_CONTENT = "content";
	
	final Keyspace keyspace;

	String columnFamilyName;

	StringSerializer ss = StringSerializer.get();

	public CassandraPersistentContent() {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
	}

	public CassandraPersistentContent(String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	public CassandraPersistentContent(String keyspaceName, String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema(keyspaceName);
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	public void set(String url, String key, String value) {

		if (columnFamilyName == null) {
			throw new IllegalStateException("Must setColumnFamilyName() first");
		}
		
		if ( ! isValidKey(key) ) {
			throw new IllegalArgumentException("The key is not allowed: " + key );
		}

		// This mutator will act over UTF8 keys (urls)
		Mutator<String> mutator = HFactory.createMutator(keyspace, ss);

		// The column sub-key is a long
		HColumn<String, String> column = HFactory.createColumn(key, value);

		mutator.insert(url, columnFamilyName, column);
		mutator.execute();
	}

	public String get(String url, String key) {

		if (columnFamilyName == null) {
			throw new IllegalStateException("Must setColumnFamilyName() first");
		}
		
		if ( ! isValidKey(key) ) {
			throw new IllegalArgumentException("The key is not allowed: " + key );
		}

		ColumnQuery<String, String, String> query = HFactory.createStringColumnQuery(keyspace);
		query.setColumnFamily(columnFamilyName);
		query.setKey(url);
		query.setName(key);
		QueryResult<HColumn<String, String>> results = query.execute();

		String response = null;
		if (results != null) {
			HColumn<String, String> result = results.get();
			response = result.getValue();
		}
		return response;
	}
	
	private boolean isValidKey(String key) {
		// return key.equals(KEY_TITLE);
		return (key.equals(KEY_TITLE) || key.equals(KEY_CONTENT));
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		if (CassandraSchema.isValidColumnFamilyNameContent(columnFamilyName)) {
			this.columnFamilyName = columnFamilyName;
		} else {
			throw new IllegalArgumentException("Not a valid column family name for content: " + columnFamilyName);
		}
	}
}
