package qa.qcri.rtsm.persist;

import qa.qcri.rtsm.persist.cassandra.CassandraPersistentTweet;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;
import qa.qcri.rtsm.twitter.SimpleTweet;

public class TweetPersister extends WriteOnlyPersister {

	final CassandraPersistentTweet cassandraTweet;
	
	public TweetPersister() {
		super();
		cassandraTweet = new CassandraPersistentTweet(CassandraSchema.COLUMNFAMILY_NAME_TWEETS);
	}
	
	public void set(String url, SimpleTweet tweet) {
		cassandraTweet.set( url, new Long(tweet.getId()), tweet ); 
	}	

	@Override
	public void set(String key, Object value, int persistTime) throws InterruptedException {
		throw new IllegalStateException("Do not call this method directly");
	}
}
