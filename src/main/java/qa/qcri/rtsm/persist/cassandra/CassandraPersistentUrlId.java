package qa.qcri.rtsm.persist.cassandra;


import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;


public class CassandraPersistentUrlId {
	public final static String KEY_URL = "url";
	
	final Keyspace keyspace;

	String columnFamilyName;

	final public static int MAX_TWEETS = Integer.MAX_VALUE;

	StringSerializer ss = StringSerializer.get();

	LongSerializer ls = LongSerializer.get();

	public CassandraPersistentUrlId() {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
	}

	public CassandraPersistentUrlId(String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	public CassandraPersistentUrlId(String keyspaceName, String columnFamilyName) {
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
		HColumn<String, String> column = HFactory.createColumn(KEY_URL, value);

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

		//System.out.println(url+" | url_id:"+results.get());
		String response = null;
		if (results != null) {
			HColumn<String, String> result = results.get();
			response = result.getValue();
		}
		return response;
	}
	
	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		if (CassandraSchema.isValidColumnFamilyUrlId(columnFamilyName)) {
			this.columnFamilyName = columnFamilyName;
		} else {
			throw new IllegalArgumentException("Not a valid column family name : " + columnFamilyName);
		}
	}
	private boolean isValidKey(String key) {
		// return key.equals(KEY_TITLE);
		return (key.equals(KEY_URL) );
	}
}
