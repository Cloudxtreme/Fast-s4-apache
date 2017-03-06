package qa.qcri.rtsm.persist.cassandra;

import java.text.ParseException;
import java.util.TreeMap;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.thrift.InvalidRequestException;

import org.json.JSONException;

import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.util.Util;

public class CassandraPersistentTweet {

	final Keyspace keyspace;

	String columnFamilyName;

	final public static int MAX_TWEETS = Integer.MAX_VALUE;

	StringSerializer ss = StringSerializer.get();

	LongSerializer ls = LongSerializer.get();

	public CassandraPersistentTweet() {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
	}

	public CassandraPersistentTweet(String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	public CassandraPersistentTweet(String keyspaceName, String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema(keyspaceName);
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	public void set(String key, Long tweetID, SimpleTweet tweet) {

		if (columnFamilyName == null) {
			throw new IllegalStateException("Must setColumnFamilyName() first");
		}

		// This mutator will act over UTF8 keys (urls)
		Mutator<String> mutator = HFactory.createMutator(keyspace, ss);

		// The column sub-key is a long
		HColumn<Long, String> column = HFactory.createColumn(tweetID, tweet.toJSON().toString());

		mutator.insert(key, columnFamilyName, column);
		mutator.execute();
	}

	public TreeMap<Long, SimpleTweet> get(String key) {

		if (columnFamilyName == null) {
			throw new IllegalStateException("Must setColumnFamilyName() first");
		}

		SliceQuery<String, Long, String> query = HFactory.createSliceQuery(keyspace, ss, ls, ss);
		query.setColumnFamily(columnFamilyName);
		query.setKey(key);
		query.setRange(new Long(Long.MIN_VALUE), new Long(Long.MAX_VALUE), false, MAX_TWEETS);
		QueryResult<ColumnSlice<Long, String>> results = query.execute();

		TreeMap<Long, SimpleTweet> response = new TreeMap<Long, SimpleTweet>();

		if (results != null) {
			ColumnSlice<Long, String> result = results.get();
			for (HColumn<Long, String> column : result.getColumns()) {
				try {
					response.put(column.getName(), new SimpleTweet(column.getValue()));
				} catch (JSONException e) {
					Util.logWarning(this, "Problem converting tweet from Cassandra (String to JSON)");
					e.printStackTrace();
				} catch (ParseException e) {
					Util.logWarning(this, "Problem parsing tweet from Cassandra (possibly date was unparseable)");
					e.printStackTrace();
				}
			}
		}
		return response;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public void setColumnFamilyName(String columnFamilyName) {
		if (CassandraSchema.isValidColumnFamilyNameTweets(columnFamilyName)) {
			this.columnFamilyName = columnFamilyName;
		} else {
			throw new IllegalArgumentException("Not a valid column family name for tweets: " + columnFamilyName);
		}
	}
}
