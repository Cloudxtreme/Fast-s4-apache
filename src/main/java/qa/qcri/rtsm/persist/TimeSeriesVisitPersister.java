package qa.qcri.rtsm.persist;

import qa.qcri.rtsm.persist.cassandra.CassandraSchema;

public class TimeSeriesVisitPersister extends TimeSeriesPersister {

	@Override
	String getColumnFamilyName() {
		return CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS;
	}

}
