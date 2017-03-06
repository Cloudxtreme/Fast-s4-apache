package qa.qcri.rtsm.persist;

import qa.qcri.rtsm.item.URLSeenParsed;
import qa.qcri.rtsm.persist.cassandra.CassandraPersistentContent;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;

public class ContentPersister extends WriteOnlyPersister {

	final CassandraPersistentContent cassandraContent;
	
	public ContentPersister() {
		super();
		cassandraContent= new CassandraPersistentContent(CassandraSchema.COLUMNFAMILY_NAME_CONTENT);
	}
	
	public void set(URLSeenParsed urlSeenParsed) {
		cassandraContent.set( urlSeenParsed.getUrl(), CassandraPersistentContent.KEY_TITLE, urlSeenParsed.getTitle() );
		cassandraContent.set( urlSeenParsed.getUrl(), CassandraPersistentContent.KEY_CONTENT, urlSeenParsed.getContent() );
	}	

	@Override
	public void set(String key, Object value, int persistTime) throws InterruptedException {
		throw new IllegalStateException("Do not call this method directly");
	}
}
