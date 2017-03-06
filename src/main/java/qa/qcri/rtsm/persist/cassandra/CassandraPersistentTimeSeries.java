package qa.qcri.rtsm.persist.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.CounterSlice;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.HCounterSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SubSliceCounterQuery;

import org.joda.time.DateTime;

import qa.qcri.rtsm.analysis.TimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries.Point;

/**
 * @author chato
 * 
 */
/**
 * @author Imran
 * 
 */
public class CassandraPersistentTimeSeries {

	final Keyspace keyspace;

	String columnFamilyName = null;

	StringSerializer ss = StringSerializer.get();

	LongSerializer ls = LongSerializer.get();

	IntegerSerializer is = IntegerSerializer.get();

	/**
     *
     */
	public CassandraPersistentTimeSeries() {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
	}

	/**
	 * 
	 * @param columnFamilyName
	 */
	public CassandraPersistentTimeSeries(String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema();
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	/**
	 * 
	 * @param keyspaceName
	 * @param columnFamilyName
	 */
	public CassandraPersistentTimeSeries(String keyspaceName, String columnFamilyName) {
		CassandraSchema cassandraSchema = new CassandraSchema(keyspaceName);
		this.keyspace = cassandraSchema.getKeyspace();
		this.setColumnFamilyName(columnFamilyName);
	}

	/**
	 * 
	 * @param columnFamilyName
	 */
	public void setColumnFamilyName(String columnFamilyName) {
		if (CassandraSchema.isValidColumnFamilyNameTimeSeries(columnFamilyName)) {
			this.columnFamilyName = columnFamilyName;
		} else {
			throw new IllegalArgumentException("Not a valid column family name for a time series: " + columnFamilyName);
		}
	}

	/**
	 * This is a slow method and should not be used.
	 * 
	 * @param part
	 * @param key
	 * @param startOfInterval
	 * @param counter
	 * @deprecated
	 */
	@Deprecated
	public void set(String part, String key, Long startOfInterval, Integer counter) {
		if (columnFamilyName == null) {
			throw new IllegalStateException("Must set columnFamilyName first");
		}

		// Do not store empty counters
		if (counter.intValue() == 0) {
			return;
		}

		// This mutator will act over UTF8 keys (urls)
		Mutator<String> mutator = HFactory.createMutator(keyspace, ss);

		// The column sub-key is a long
		HCounterColumn<Long> column = HFactory.createCounterColumn(startOfInterval, counter.longValue(), ls);
		@SuppressWarnings("unchecked")
		// The super-column key is a UTF8
		HCounterSuperColumn<String, Long> superColumn = HFactory.createCounterSuperColumn(part, Arrays.asList(column), ss, ls);

		// Insert
		mutator.insertCounter(key, columnFamilyName, superColumn);
		mutator.execute();
	}

	/**
	 * 
	 * @param part
	 * @param key
	 * @param counters
	 */
	public void set(String part, String key, TreeMap<Long, Integer> counters) {
		if (columnFamilyName == null) {
			throw new IllegalStateException("Must set columnFamilyName first");
		}
		if( counters.size() == 0 ) {
			return;
		}

		// This mutator will act over UTF8 keys (urls)
		Mutator<String> mutator = HFactory.createMutator(keyspace, ss);
		Vector<HCounterColumn<Long>> columns = new Vector<HCounterColumn<Long>>(counters.size());

		for (Long startOfInterval : counters.keySet()) {
			Integer counter = counters.get(startOfInterval);
			if (counter.intValue() > 0) {
				// The column sub-key is a long
				HCounterColumn<Long> column = HFactory.createCounterColumn(startOfInterval, counter.longValue(), ls);
				columns.add(column);
			}
		}

		// The super-column key is a UTF8
		HCounterSuperColumn<String, Long> superColumn = HFactory.createCounterSuperColumn(part, columns, ss, ls);

		// Insert
		mutator.insertCounter(key, columnFamilyName, superColumn);
		mutator.execute();
	}

	/**
	 * 
	 * @param part
	 * @param key
	 * @param startOfInterval
	 * @return
	 */
	public Integer get(String part, String key, Long startOfInterval) {
		SubSliceCounterQuery<String, String, Long> subSliceCounterQuery = HFactory.createSubSliceCounterQuery(keyspace, ss, ss, ls);
		subSliceCounterQuery.setColumnFamily(columnFamilyName);
		subSliceCounterQuery.setSuperColumn(part);
		subSliceCounterQuery.setKey(key);
		subSliceCounterQuery.setColumnNames(startOfInterval);

		QueryResult<CounterSlice<Long>> results = subSliceCounterQuery.execute();
		HCounterColumn<Long> result = results.get().getColumnByName(startOfInterval);
		if (result == null) {
			return null;
		} else {
			// I am not sure why the counter is returned as long, we cast it to integer
			return new Integer(result.getValue().intValue());
		}
	}

	/**
	 * 
	 * @param part
	 * @param key
	 * @return
	 */
	public TreeMap<Long, Integer> get(String part, String key) {
		SubSliceCounterQuery<String, String, Long> subSliceCounterQuery = HFactory.createSubSliceCounterQuery(keyspace, ss, ss, ls);
		subSliceCounterQuery.setColumnFamily(columnFamilyName);
		subSliceCounterQuery.setSuperColumn(part);
		subSliceCounterQuery.setKey(key);
		subSliceCounterQuery.setRange(new Long(Long.MIN_VALUE), new Long(Long.MAX_VALUE), false, Integer.MAX_VALUE);

		QueryResult<CounterSlice<Long>> results = subSliceCounterQuery.execute();
		List<HCounterColumn<Long>> columns = results.get().getColumns();
		TreeMap<Long, Integer> response = new TreeMap<Long, Integer>();
		for (HCounterColumn<Long> column : columns) {

			// I am not sure why the counter is returned as long, we cast it to integer
			response.put(column.getName(), new Integer(column.getValue().intValue()));
		}
		return response;
	}

	/**
	 * 
	 * @param part
	 * @param key
	 * @param visitsLowerBound
	 * @return
	 */
	public TreeMap<Long, Integer> get(String part, String key, Integer visitsLowerBound) {

		boolean responseQualified = false;
		SubSliceCounterQuery<String, String, Long> subSliceCounterQuery = HFactory.createSubSliceCounterQuery(keyspace, ss, ss, ls);
		subSliceCounterQuery.setColumnFamily(columnFamilyName);
		subSliceCounterQuery.setSuperColumn(part);
		subSliceCounterQuery.setKey(key);
		subSliceCounterQuery.setRange(new Long(Long.MIN_VALUE), new Long(Long.MAX_VALUE), false, Integer.MAX_VALUE);

		QueryResult<CounterSlice<Long>> results = subSliceCounterQuery.execute();
		List<HCounterColumn<Long>> columns = results.get().getColumns();
		TreeMap<Long, Integer> response = new TreeMap<Long, Integer>();
		for (HCounterColumn<Long> column : columns) {
			Integer value = column.getValue().intValue();
			if (value >= visitsLowerBound) {
				responseQualified = true;
			}
			long time = (long) column.getName();
			response.put(time, value);

		}
		if (responseQualified) {
			return response;

		} else {
			return null;
		}
	}
	

	public TimeSeries getTimeSeries(String key, String part) {
		SubSliceCounterQuery<String, String, Long> subSliceCounterQuery = HFactory.createSubSliceCounterQuery(keyspace, ss, ss, ls);
		subSliceCounterQuery.setColumnFamily(columnFamilyName);
		subSliceCounterQuery.setSuperColumn(part);
		subSliceCounterQuery.setKey(key);
		subSliceCounterQuery.setRange(new Long(Long.MIN_VALUE), new Long(Long.MAX_VALUE), false, Integer.MAX_VALUE);

		QueryResult<CounterSlice<Long>> results = subSliceCounterQuery.execute();
		List<HCounterColumn<Long>> columns = results.get().getColumns();
	
		TimeSeries ts = new TimeSeries(key + "-" + part);
		
		for (HCounterColumn<Long> column : columns) {
			ts.insertPoint(new Point(column.getName(), new Double(column.getValue().doubleValue())));
		}
		return ts;
	}

	/**
	 * 
	 * @param domain
	 * @return
	 */
	public List<String> getAllArticles(String domain) {

		List<String> keysList = new ArrayList<String>();
		CqlQuery<String, String, Long> cqlQuery = new CqlQuery<String, String, Long>(keyspace, ss, ss, ls);
		cqlQuery.setQuery("SELECT key FROM " + columnFamilyName);
		QueryResult<CqlRows<String, String, Long>> result = cqlQuery.execute();

		if (result != null && result.get() != null) {
			List<Row<String, String, Long>> list = result.get().getList();
			for (Row row : list) {
				String key = row.getKey().toString();
				if (isDomainRelated(key, domain))// checking if key domain and input domain are same
				{
					keysList.add(key);
				}
			}
		}

		return keysList;

	}

	/**
	 * This method returns a list of articles filtered based on following logic: What we want are
	 * two dates A and B and a set of articles P such that: - (1) The date A should be ~1 day after
	 * the beginning of the observations (the older date for which you have a visit recorded in the
	 * data) (2) The date B should be ~3-4 days before the end of the observations (today) (3) The
	 * articles in P do not have any visit before A (4) The articles in P have at least 100 visits
	 * between A and B.
	 * 
	 * @param domain
	 * @param interval
	 */
	public TreeMap<String, Integer> filterArticles2(String domain, String interval) {
		TreeMap<String, Integer> rspMap = new TreeMap<String, Integer>();
		ValueComparator valueCom = new ValueComparator(rspMap);
		TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(valueCom);

		CqlQuery<String, String, Long> cqlQuery = new CqlQuery<String, String, Long>(keyspace, ss, ss, ls);
		cqlQuery.setQuery("SELECT key FROM " + columnFamilyName);
		QueryResult<CqlRows<String, String, Long>> result = cqlQuery.execute();

		Long startDate = 0l;
		Long endDate = 0l;
		if (result != null && result.get() != null) {
			List<Row<String, String, Long>> list = result.get().getList();
			for (Row row : list) {
				String key = row.getKey().toString();
				if (isDomainRelated(key, domain))// checking if the article's domain matches the
													// input domain
				{
					TreeMap<Long, Integer> valuesMap = get(interval, key);
					startDate = new DateTime(valuesMap.firstKey()).plusDays(1).getMillis();
					endDate = new DateTime(valuesMap.lastKey()).minusDays(3).getMillis();
					TreeMap<Long, Integer> croppedMap = cropSeries(valuesMap, startDate, endDate);
					int totalVisits = calculateVisitsSum(croppedMap);
					if (totalVisits > 100) {
						rspMap.put(key, totalVisits);
					}
				}
			}
		}

		sortedMap.putAll(rspMap);
		return sortedMap;

	}

	private TreeMap<Long, Integer> cropSeries(TreeMap<Long, Integer> map, Long startDate, Long endDate) {
		TreeMap<Long, Integer> responseMap = (TreeMap<Long, Integer>) map.clone();
		for (Map.Entry<Long, Integer> entry : map.entrySet()) {
			if (startDate >= entry.getKey()) {
				responseMap.remove(entry.getKey()); // removing elements from the beginning which
													// are before the startDate
			}
			if (entry.getKey() >= endDate) {
				responseMap.remove(entry.getKey()); // removing elements from the end which are
													// after the endDate
			}
		}
		return responseMap;
	}

	/**
	 * For a given domain and a time-interval this method returns a list of articles and their
	 * visits/FB-shares based on the highest number of visits/FB-shares.
	 * 
	 * @param domain for example ALJAZEERA OR any other domain. Please use StaticVars class to
	 *            select one domain.
	 * @param part The time-interval v_1m etc
	 * @return
	 */
	public TreeMap<String, Integer> getTopArticles(String domain, String part) {

		TreeMap<String, Integer> rspMap = new TreeMap<String, Integer>();
		ValueComparator valueCom = new ValueComparator(rspMap);
		TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(valueCom);

		CqlQuery<String, String, Long> cqlQuery = new CqlQuery<String, String, Long>(keyspace, ss, ss, ls);
		cqlQuery.setQuery("SELECT key FROM " + columnFamilyName);
		QueryResult<CqlRows<String, String, Long>> result = cqlQuery.execute();

		if (result != null && result.get() != null) {
			List<Row<String, String, Long>> list = result.get().getList();
			for (Row row : list) {
				String key = row.getKey().toString();
				if (isDomainRelated(key, domain))// checking if key domain and input domain are same
				{
					TreeMap<Long, Integer> valuesMap = get(part, key);
					int totalVisits = calculateVisitsSum(valuesMap);
					rspMap.put(key, totalVisits);
				}
			}
		}

		sortedMap.putAll(rspMap);
		return sortedMap;
	}

	/**
	 * This method is used to get a filtered list of articles based on two factors: is the article
	 * too old? AND is the article too new to be considered. This method is not yet completed so use
	 * on your own risk.
	 * 
	 * @param domain
	 * @param part
	 * @return
	 */
	public TreeMap<String, Integer> filterArticles(String domain, String part) {

		TreeMap<String, Integer> rspMap = new TreeMap<String, Integer>();
		ValueComparator valueCom = new ValueComparator(rspMap);
		TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(valueCom);

		CqlQuery<String, String, Long> cqlQuery = new CqlQuery<String, String, Long>(keyspace, ss, ss, ls);
		cqlQuery.setQuery("SELECT key FROM " + columnFamilyName);
		QueryResult<CqlRows<String, String, Long>> result = cqlQuery.execute();

		if (result != null && result.get() != null) {
			List<Row<String, String, Long>> list = result.get().getList();
			System.out.println("Total articles in database: " + list.size());
			for (Row row : list) {
				String key = row.getKey().toString();
				if (isDomainRelated(key, domain))// checking if key domain and input domain are same
				{
					TreeMap<Long, Integer> valuesMap = get(part, key);
					if (!isOldArticle(valuesMap) && !isTooNewArticle(valuesMap)) {
						int totalVisits = calculateVisitsSum(valuesMap);
						rspMap.put(key, totalVisits);
					}
				}
			}
		}

		sortedMap.putAll(rspMap);
		return sortedMap;
	}

	private boolean isOldArticle(TreeMap<Long, Integer> visitsMap) {

		// Determining if the article is old, that is if an atricle has more than zero visits on his
		// first day in DB AND
		// if it does not get any visits during first day
		boolean isFirstVisitGTZero = false;
		boolean isSeenFirstDay = false;
		int visits = 0;
		if (visitsMap.get(visitsMap.firstKey()) > 1) {
			isFirstVisitGTZero = true; // yes its visits are greater than zero
			DateTime t2 = new DateTime(visitsMap.firstKey()).plusDays(1);
			for (Map.Entry<Long, Integer> entry : visitsMap.entrySet()) {
				if (entry.getKey() >= t2.getMillis()) {
					if (visits - visitsMap.get(visitsMap.firstKey()) > 0) {
						isSeenFirstDay = true; // Yes
						break;
					}
				} else {
					visits += entry.getValue();
				}

			}
		}

		if (isFirstVisitGTZero && !isSeenFirstDay) {
			return true;
		} else {
			return false;
		}

	}

	private boolean isTooNewArticle(TreeMap<Long, Integer> map) {
		// if an article has less than 7 days data then it is too new to be considered
		DateTime t1 = new DateTime(map.firstKey());
		DateTime t2 = new DateTime(map.lastKey());

		if (t1.plusDays(7).getMillis() >= t2.getMillis()) {
			return false;
		} else {
			return true;
		}
	}

	private int calculateVisitsSum(TreeMap<Long, Integer> visits) {

		int result = 0;
		for (Map.Entry<Long, Integer> entry : visits.entrySet()) {
			result += entry.getValue();
		}
		return result;
	}

	/**
	 * 
	 * @param limit
	 * @param domain
	 * @return
	 */
	public List<String> getNKeys(Integer limit, String domain) {

		List<String> keysList = new ArrayList<String>();
		CqlQuery<String, String, Long> cqlQuery = new CqlQuery<String, String, Long>(keyspace, ss, ss, ls);
		cqlQuery.setQuery("SELECT key FROM " + columnFamilyName + " LIMIT " + limit);
		QueryResult<CqlRows<String, String, Long>> result = cqlQuery.execute();

		if (result != null && result.get() != null) {
			List<Row<String, String, Long>> list = result.get().getList();
			for (Row row : list) {
				// List columns = row.getColumnSlice().getColumns();
				String key = row.getKey().toString();
				if (isDomainRelated(key, domain))// checking if key domain and input domain are same
				{
					keysList.add(key);
				}
				// for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
				// HColumn column = (HColumn) iterator.next();
				// System.out.print(column.getName() + ":" + column.getValue()
				// + "\t");
				// }
			}
		}

		return keysList;

	}

	private boolean isDomainRelated(String key, String domain) {

		if (key.length() >= domain.length()) {
			String keyDomain = key.substring(0, domain.length());
			if (keyDomain.equalsIgnoreCase(domain)) {
				return true;
			}
		}

		return false;

	}
}

class ValueComparator implements Comparator<String> {

	Map<String, Integer> base;

	public ValueComparator(Map<String, Integer> base) {
		this.base = base;
	}

	// Note: this comparator imposes orderings that are inconsistent with equals.
	public int compare(String a, String b) {
		if (base.get(a) >= base.get(b)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}
}