/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
package qa.qcri.rtsm.analysis.imran;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import qa.qcri.rtsm.analysis.TimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries.EmptySeriesException;
import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.draw.charts.DrawMovingAvgChart;
import qa.qcri.rtsm.draw.charts.DrawTimeSeriesChart;
import qa.qcri.rtsm.draw.charts.DrawTimeSeriesLineCharts;
import qa.qcri.rtsm.persist.cassandra.CassandraPersistentTweet;
import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.ui.StaticVars;

/**
 * 
 * @author Imran
 */
public class TweetsAnalysis {

	private CassandraPersistentTweet tweetTimeSeries;

	/**
     *
     */
	public TweetsAnalysis() {
	}

	/**
	 * 
	 * @return
	 */
	public CassandraPersistentTweet getTweetsKeyspace() {
		tweetTimeSeries = new CassandraPersistentTweet("Tweet");
		return tweetTimeSeries;
	}
	
	public TimeSeries getTimeSeries(String key) throws EmptySeriesException {
		TimeSeries ts = new TimeSeries(key);
		SortedMap<Long, SimpleTweet> tweets = tweetTimeSeries.get(key);
		if( tweets != null && tweets.size() > 0 ) {
			for( SimpleTweet tweet: tweets.values() ) {
				long date = tweet.getCreatedAt().getTime();
				ts.addPoint( new Point(new Long(date), new Double(1) ) );
			}
    	} else {
    		throw new TimeSeries.EmptySeriesException();
    	}
		return ts;
	}
	
	public TimeSeries getTimeSeries(String key, int resolutionSeconds) throws EmptySeriesException {
		long resolutionMillis = resolutionSeconds * 1000;
		TimeSeries ts = new TimeSeries(key);
		SortedMap<Long, SimpleTweet> tweets = tweetTimeSeries.get(key);
		if( tweets != null && tweets.size() > 0 ) {
			for( SimpleTweet tweet: tweets.values() ) {
				long date = tweet.getCreatedAt().getTime();
				long dateRounded = date - ( date % resolutionMillis );
				ts.addPoint( new Point(new Long(dateRounded), new Double(1) ) );
			}
    	} else {
    		throw new TimeSeries.EmptySeriesException();
    	}
		return ts;
	}

	// public List<TweetsTimeSeriesSorted> getNTweetsTimeSeries(Integer limit, String domain) {
	//
	// List<TweetsTimeSeriesSorted> tweetTSSorted = tweetTimeSeries.getNTweetSeries(limit, domain);
	//
	// return tweetTSSorted;
	// }
	/**
	 * This method constructs the accumulative time series for a given article's URL (i.e., key).
	 * 
	 * @param key an article's URL
	 * @return an object of the TimeSeriesSorted class
	 */
	public TimeSeriesSorted constructAccumulativeTweetsTS(String key) {

		TimeSeriesSorted ts = new TimeSeriesSorted();
		SortedMap<Long, Integer> responseMap = new TreeMap<Long, Integer>();
		Integer tweets = 1;
		SortedMap<Long, SimpleTweet> map = tweetTimeSeries.get(key);
		if (!(map.isEmpty())) {
			for (Map.Entry<Long, SimpleTweet> entry : map.entrySet()) {
				SimpleTweet tweetObj = entry.getValue();
				long newTweetTime = tweetObj.getCreatedAt().getTime();
				if (responseMap.containsKey(newTweetTime)) { // check if map has already the same
																// key
					Integer mapTweetsCounter = responseMap.get(newTweetTime); // getting the old
																				// tweets counter
																				// from the map
					responseMap.put(newTweetTime, 1 + mapTweetsCounter); // increment tweets coutner
																			// by 1
				} else {
					responseMap.put(newTweetTime, tweets);
				}

				tweets++;
			}

			ts.setKey(key);
			ts.setAbsoluteSeries(responseMap);
		}

		return ts;
	}

	/**
	 * This method constructs the relative time series from an absolute series based on the given
	 * time-interval and time-format.
	 * 
	 * @param timeSeries An object of TimeSeriesSorted class which a valid absolute time series
	 * @param timeInterval It is the distance (in time) between two points in the relative time
	 *            series.
	 * @param timeFormat The time-format that is used to construct the relative timeseries. A list
	 *            of valid time-formats are given in staticVars class.
	 * @return
	 */
	public TimeSeriesSorted constructRelativeSeries(TimeSeriesSorted timeSeries, int timeInterval, String timeFormat) {

		TimeSeriesSorted ts = new TimeSeriesSorted();
		SortedMap<Long, Integer> absoluteMap = timeSeries.getAbsoluteSeries();
		SortedMap<Long, Integer> relativeMap = new TreeMap<Long, Integer>();
		long t1 = 0;
		if (absoluteMap != null) {
			for (Map.Entry<Long, Integer> entry : absoluteMap.entrySet()) {

				Long t2 = entry.getKey();
				if (absoluteMap.firstKey().equals(t2)) { // checking if map is at its first index
					t1 = t2;
				}
				boolean timeCond = true;
				DateTime time1 = new DateTime(t1);
				DateTime time2 = new DateTime(t2);
				while (timeCond) {
					if (time1.getMillis() < time2.getMillis()) {
						relativeMap.put(time1.getMillis() + 1, 0);
						time1 = getTimeIncrement(time1, timeInterval, timeFormat);

					} else {

						Integer absoluteValueT1 = absoluteMap.get(t1);
						Integer absoluteValueT2 = absoluteMap.get(t2);
						Integer relativeValueT2 = 0;
						if (absoluteValueT2 >= absoluteValueT1) {
							relativeValueT2 = absoluteValueT2 - absoluteValueT1;
						}
						relativeMap.put(t2, relativeValueT2);
						t1 = t2;
						timeCond = false;
					}
				}
			}

			ts.setAbsoluteSeries(timeSeries.getAbsoluteSeries());
			ts.setRelativeSeries(relativeMap.subMap(relativeMap.firstKey(), relativeMap.lastKey()));
		} else {
			System.out.println("No tweets found");
		}

		ts.setKey(timeSeries.getKey());
		return ts;
	}

	private DateTime getTimeIncrement(DateTime time, int interval, String timeFormat) {

		if (timeFormat.equals(StaticVars.TIME_FORMAT_MIN)) {
			return time.plusMinutes(interval);
		} else if (timeFormat.equals(StaticVars.TIME_FORMAT_DAY)) {
			return time.plusDays(interval);
		} else if (timeFormat.equals(StaticVars.TIME_FORMAT_HOUR)) {
			return time.plusHours(interval);
		}

		return null;
	}

	/**
	 * Old method and should not be used.
	 * 
	 * @param keys
	 * @return
	 */
	@Deprecated
	public List<TimeSeriesSorted> getConstructedTweetsSeries(List<String> keys) {

		List<TimeSeriesSorted> tsList = new ArrayList<TimeSeriesSorted>();
		SortedMap<Long, Integer> responseMap = new TreeMap<Long, Integer>();

		for (String key : keys) {
			System.out.println("Key :" + key);
			TreeMap<Long, SimpleTweet> map = tweetTimeSeries.get(key);
			// SortedMap<Long, SimpleTweet> sortedmap = map.subMap(map.firstKey(), map.lastKey());

			boolean firstIteration = true;
			Integer tweets = 1;
			Long startTime = 0l;
			Long nextTime = 0l;
			for (Map.Entry<Long, SimpleTweet> entry : map.entrySet()) {
				long time = entry.getKey();
				if (firstIteration) {
					startTime = time;
					firstIteration = false;
				}
				nextTime = time;
				if (getDiff(startTime, nextTime)) {
					System.out.println("I got tweets: " + tweets);
					responseMap.put(nextTime, tweets);
					startTime = nextTime;
				} else {
					tweets++;
				}
			}

			responseMap.put(nextTime, tweets);
			TimeSeriesSorted ts = new TimeSeriesSorted();
			ts.setKey(key);
			ts.setRelativeSeries(responseMap);
			tsList.add(ts);
			// responseMap.clear();

		}
		return tsList;

	}

	private boolean getDiff(Long startTime, Long endTime) {
		System.out.println(startTime + " " + endTime);
		Long diff = endTime - startTime;
		System.out.println("Difference in time: " + diff);
		// Calculate difference in seconds
		// long diff = diff / 1000;

		// Calculate difference in minutes
		diff = diff / (60 * 1000);

		// Calculate difference in hours
		// diff = diff / (60 * 60 * 1000);

		// Calculate difference in days
		// diff = diff / (24 * 60 * 60 * 1000);

		if (diff >= 1) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 
	 * @param sortedTS
	 */
	public void drawLineChart(List<TimeSeriesSorted> sortedTS) {

		DrawTimeSeriesLineCharts draw = new DrawTimeSeriesLineCharts("URL Visits");
		draw.drawTimeSeriesURLVisitLineChart(sortedTS);
	}

	/**
	 * 
	 * @param sortedTS
	 * @param movAvgPointsTxtB
	 * @param legend
	 */
	public void drawMovAvgChart(List<TimeSeriesSorted> sortedTS, Integer movAvgPointsTxtB, boolean legend) {

		DrawMovingAvgChart movAvgChart = new DrawMovingAvgChart();
		movAvgChart.drawTimeSeriesMovAvgChart(sortedTS, movAvgPointsTxtB, "Tweets: Moving Avg.", "Data & Time", "Tweets", legend);
	}

	/**
	 * 
	 * @param sortedTS
	 * @param seriesType
	 */
	public void drawTSChart(List<TimeSeriesSorted> sortedTS, String seriesType) {

		DrawTimeSeriesChart demo = new DrawTimeSeriesChart("Time Series Chart");
		demo.drawTimeSeriesURLVisitTSChart(sortedTS, seriesType, "Tweets: Time Series.", "Data & Time", "Tweets", false);

	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	public String printDate(long input) {
		DateTime time = new DateTime(input, DateTimeZone.forTimeZone(TimeZone.getTimeZone("AST")));
		return time.toString();

	}

	/**
	 * 
	 * @param argus
	 */
	public static void main(String argus[]) {
		TweetsAnalysis analysisDriver = new TweetsAnalysis();
		analysisDriver.getTweetsKeyspace();

	}
}
