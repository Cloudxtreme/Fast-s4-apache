/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.analysis.imran;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;

import qa.qcri.rtsm.analysis.TimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries.EmptySeriesException;
import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.draw.charts.DrawMovingAvgChart;
import qa.qcri.rtsm.draw.charts.DrawTimeSeriesChart;
import qa.qcri.rtsm.draw.charts.DrawTimeSeriesLineCharts;
import qa.qcri.rtsm.persist.cassandra.CassandraPersistentTimeSeries;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;
import qa.qcri.rtsm.ui.StaticVars;

/**
 *
 * @author Imran
 */
public class FacebookShareAnalysis {

    private CassandraPersistentTimeSeries timeSeries;

    /**
     *
     * @return
     */
    public CassandraPersistentTimeSeries getTimeSeriesKeyspace() {
        timeSeries = new CassandraPersistentTimeSeries(CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_FACEBOOK);
        return timeSeries;
    }

    /**
     *
     * @param keys the list of articles URL
     * @param timeInterval the time-intervals that are being used in the
     * application e.g., 1m, 1h, 10s (each corresponds to cassandra supercolumn)
     * @return
     */
    public List<TimeSeriesSorted> getTimeSeriesColumnValues(List<String> keys, String timeInterval) {

        String superColumn = timeInterval;
        Iterator itr = keys.iterator();
        List<TimeSeriesSorted> timeSeriesList = new ArrayList<TimeSeriesSorted>();
        System.out.println(keys.size());
        while (itr.hasNext()) {

            String key = (String) itr.next();
            TreeMap<Long, Integer> seriesData = timeSeries.get(superColumn, key);
            System.out.println(seriesData.size());
            SortedMap<Long, Integer> sortedMap = seriesData.subMap(seriesData.firstKey(), seriesData.lastKey());
            TimeSeriesSorted timeSeries = new TimeSeriesSorted(key);
            timeSeries.setAbsoluteSeries(sortedMap);
            timeSeriesList.add(timeSeries);
        }

        return timeSeriesList;
    }

    /**
     *
     * @param key an article's URL
     * @param timeInterval  time-intervals that are being used in the
     * application i.e., 1m, 1h, 10s (each corresponds to cassandra supercolumn)
     * @param visitsLowerBound the minimum threshold value of the Facebook shares
     * @return
     */
    public TimeSeriesSorted getTimeSeriesColumnValues(String key, String timeInterval, Integer visitsLowerBound) {

        String superColumn = timeInterval;
        TimeSeriesSorted ts = new TimeSeriesSorted();
        ts.setKey(key);
        TreeMap<Long, Integer> seriesData = timeSeries.get(superColumn, key, visitsLowerBound);
        if (seriesData != null) {
            SortedMap<Long, Integer> sortedMap = seriesData.subMap(seriesData.firstKey(), true, seriesData.lastKey(), true);
            ts.setAbsoluteSeries(sortedMap);
        }
        return ts;
    }
    
    public TimeSeries getTimeSeries(String url, String timeInterval, Integer visitsLowerBound) throws EmptySeriesException {
    	String superColumn = timeInterval;
        TreeMap<Long, Integer> seriesData = timeSeries.get(superColumn, url, visitsLowerBound);
        if (seriesData != null) {
            TimeSeries ts = new TimeSeries(url + "-" + timeInterval);
            for( Entry<Long, Integer> entry: seriesData.entrySet() ) {
            	ts.insertPoint(new Point(entry.getKey(), new Double(entry.getValue().doubleValue())));
            }
            return ts;
        } else {
        	throw new TimeSeries.EmptySeriesException();
        }
    }

    /**
     * 
     * @param timeSeries
     * @param timeInterval
     * @param timeFormat
     * @return
     * @deprecated
     */
    @Deprecated
    public TimeSeriesSorted constructRelativeSeries(TimeSeriesSorted timeSeries, int timeInterval, String timeFormat) {
        
        TimeSeriesSorted ts = new TimeSeriesSorted();
        SortedMap<Long, Integer> absoluteMap = timeSeries.getAbsoluteSeries();
        SortedMap<Long, Integer> relativeMap = new TreeMap<Long, Integer>();
        int mapPosition = 0;
        long t1 = 0;
        for (Map.Entry<Long, Integer> entry : absoluteMap.entrySet()) {

            Long t2 = entry.getKey();
            if (mapPosition == 0) {
                t1 = t2;
                relativeMap.put(t1, 0);
                mapPosition++;
            }

            if (getTimeDiff(t1, t2, timeFormat) >= timeInterval) {
                Integer absoluteValueT1 = absoluteMap.get(t1);
                Integer absokuteValueT2 = absoluteMap.get(t2);
                Integer relativeValueT2 = absokuteValueT2 - absoluteValueT1;
                relativeMap.put(t2, relativeValueT2);
                t1 = t2;
            } else {
                relativeMap.put(t2, 0);
            }

        }

        ts.setAbsoluteSeries(timeSeries.getAbsoluteSeries());
        ts.setRelativeSeries(relativeMap);
        ts.setKey(timeSeries.getKey());
        return ts;
    }

    //Use this method to construct relative series
    /**
     * 
     * @param timeSeries the instance of TimeSeriesSorted class for which absolute series has been calculated
     * @param timeInterval the time-interval between two points in a series
     * @param timeFormat the time-format which will be used to construct relative time series. Use predefined time-formats in StaticVars class.
     * @return
     */
    public TimeSeriesSorted constructRelativeSeries2(TimeSeriesSorted timeSeries, int timeInterval, String timeFormat) {

        TimeSeriesSorted ts = new TimeSeriesSorted();
        SortedMap<Long, Integer> absoluteMap = timeSeries.getAbsoluteSeries();
        if( absoluteMap == null ) {
        	return null;
        }
        
        SortedMap<Long, Integer> relativeMap = new TreeMap<Long, Integer>();
        long t1 = Long.MIN_VALUE;
        for (Map.Entry<Long, Integer> entry : absoluteMap.entrySet()) {

            Long t2 = entry.getKey();
            if (absoluteMap.firstKey().equals(t2)) { //checking if map is at its first index
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
        ts.setRelativeSeries(relativeMap);

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
        } else if (timeFormat.equals(StaticVars.TIME_FORMAT_SECONDS)){
            return time.plusSeconds(interval);
        }

        return null;
    }

    
    private int getTimeDiff(Long time1, Long time2, String timeFormat) {
        DateTime t1 = new DateTime(time1);
        DateTime t2 = new DateTime(time2);
        int result = 0;
        if (timeFormat.equals(StaticVars.TIME_FORMAT_MIN)) {
            result = Minutes.minutesBetween(t1, t2).getMinutes();
        } else if (timeFormat.equals(StaticVars.TIME_FORMAT_DAY)) {
            result = Days.daysBetween(t1, t2).getDays();
        } else if (timeFormat.equals(StaticVars.TIME_FORMAT_HOUR)) {
            result = Hours.hoursBetween(t1, t2).getHours();
        }

        return result;

    }
    
     public void printArticleByVisits(String domain, String superColumn){
        
        TreeMap<String, Integer> seriesData = timeSeries.getTopArticles(domain, superColumn);
        for(Map.Entry<String, Integer> entry: seriesData.entrySet()){
            
            System.out.println(entry.getValue() + " - " + entry.getKey());
        }
    }

    /**
     * 
     * @param tsList
     * @return
     */
    @Deprecated
    public List<TimeSeriesSorted> getAveNormalizedSeries(List<TimeSeriesSorted> tsList) {
        Integer averagePoint = 0;
        List<TimeSeriesSorted> rspTSList = new ArrayList<TimeSeriesSorted>();
        Iterator tsITR = tsList.iterator();
        while (tsITR.hasNext()) {
            TimeSeriesSorted ts = (TimeSeriesSorted) tsITR.next();
            SortedMap<Long, Integer> map = ts.getRelativeSeries();
            Integer totalVisits = 0;
            Integer totalPoints = 0;
            //computing average point
            for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                totalVisits += entry.getValue();
                totalPoints++;
            }
            averagePoint = totalVisits / totalPoints;
            ts.setAveragePoint(averagePoint);
            TreeMap<Long, Integer> normalizedMap = new TreeMap<Long, Integer>();

            // normalizing series by dividing with the average point
            for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                normalizedMap.put(entry.getKey(), entry.getValue() / averagePoint);
            }
            ts.setRelativeSeries(normalizedMap);
            rspTSList.add(ts);
            totalPoints = 0;
            totalVisits = 0;
        }
        return rspTSList;
    }

    /**
     * 
     * @param ts
     * @return
     */
    public TimeSeriesSorted getAveNormalizedSeries(TimeSeriesSorted ts) {
        Float averagePoint = 0f;
        SortedMap<Long, Integer> map = ts.getRelativeSeries();
        Float totalVisits = 0f;
        Float totalPoints = 0f;
        //computing average point
        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            totalVisits += entry.getValue();
            if (entry.getValue() > 0) {
                totalPoints++;
            }
        }
        averagePoint = totalVisits / totalPoints;
        SortedMap<Long, Float> normalizedMap = new TreeMap<Long, Float>();
        // normalizing series by dividing with the average point
        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            Integer value = entry.getValue();
                Float res = value / averagePoint;
                normalizedMap.put(entry.getKey(), res);
        }
        ts.setAveragePoint(averagePoint);
        ts.setNormalizedSeries(normalizedMap);
        return ts;
    }

    /**
     * 
     * @param tsList
     */
    public void printTimeSeriesSorted(List<TimeSeriesSorted> tsList) {

        Iterator itr = tsList.iterator();
        while (itr.hasNext()) {

            TimeSeriesSorted ts = (TimeSeriesSorted) itr.next();
            System.out.println("Key: " + ts.getKey());
            // System.out.println("Avg: " + ts.getAveragePoint());
            SortedMap<Long, Integer> map = ts.getRelativeSeries();
            for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                System.out.println("Time-Shares: " + entry.getKey() + " (" + printDate(entry.getKey()) + " ) - " + entry.getValue());
            }
        }
    }

    /**
     * 
     * @param sortedTS
     */
    public void drawLineChart(List<TimeSeriesSorted> sortedTS) {

        DrawTimeSeriesLineCharts draw = new DrawTimeSeriesLineCharts("Line Chart");
        draw.drawTimeSeriesURLVisitLineChart(sortedTS);
    }

    /**
     * 
     * @param sortedTS
     * @param movAvgPoints
     * @param legend
     */
    public void drawMovAvgChart(List<TimeSeriesSorted> sortedTS, Integer movAvgPoints, boolean legend) {

        DrawMovingAvgChart movAvgChart = new DrawMovingAvgChart();
        movAvgChart.drawTimeSeriesMovAvgChart(sortedTS, movAvgPoints, "FB Shares: Moving Avg.", "Data & Time", "Shares", legend);
    }

    /**
     * 
     * @param sortedTS
     * @param seriesType
     */
    public void drawTSChart(List<TimeSeriesSorted> sortedTS, String seriesType) {

        DrawTimeSeriesChart demo = new DrawTimeSeriesChart("");
        demo.drawTimeSeriesURLVisitTSChart(sortedTS, seriesType, "FB Shares: Time Series", "Data & Time", "Shares", false);

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

    public static void main(String argus[]) {

        FacebookShareAnalysis analysisDriver = new FacebookShareAnalysis();
        analysisDriver.getTimeSeriesKeyspace();
        analysisDriver.printArticleByVisits(StaticVars.ALJAZEERA_DOMAIN, TimeSeriesIntervals.KEY_ONE_MINUTE_FB);

    }
}
