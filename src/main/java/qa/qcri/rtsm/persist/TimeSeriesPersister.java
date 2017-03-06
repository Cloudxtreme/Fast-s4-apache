package qa.qcri.rtsm.persist;

import java.util.TreeMap;

import qa.qcri.rtsm.persist.cassandra.CassandraPersistentTimeSeries;

public abstract class TimeSeriesPersister extends WriteOnlyPersister {

	final CassandraPersistentTimeSeries cassandraTimeSeries;

	abstract String getColumnFamilyName();
	
	public TimeSeriesPersister() {
		super();
		cassandraTimeSeries = new CassandraPersistentTimeSeries(getColumnFamilyName());
	}
	
	public void set(String part, String key, TreeMap<Long, Integer> counters) {
		cassandraTimeSeries.set(part, key, counters);
	}	

	@Override
	public void set(String key, Object value, int persistTime) throws InterruptedException {
		throw new IllegalStateException("Do not call this method directly");
	}
}
