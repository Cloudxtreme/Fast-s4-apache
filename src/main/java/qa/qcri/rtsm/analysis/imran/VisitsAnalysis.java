/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.analysis.imran;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Weeks;

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
public class VisitsAnalysis {

    private CassandraPersistentTimeSeries timeSeries;

    /**
     *
     * @return
     */
    public CassandraPersistentTimeSeries getTimeSeriesKeyspace() {
        timeSeries = new CassandraPersistentTimeSeries(CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
        return timeSeries;
    }
    
    /**
     *
     * @param key an article's URL
     * @param timeInterval time-intervals that are being used in the application
     * i.e., v_1m, v_1h, v_10s (each corresponds to cassandra supercolumn)
     * @return TimeSeriesSorted
     */
    public TimeSeriesSorted getTimeSeriesColumnValues(String key, String timeInterval) {

        String superColumn = timeInterval;
        TimeSeriesSorted ts = new TimeSeriesSorted();
        TreeMap<Long, Integer> seriesData = timeSeries.get(superColumn, key);
        SortedMap<Long, Integer> sortedMap = seriesData.subMap(seriesData.firstKey(), seriesData.lastKey());
        ts.setKey(key);
        ts.setRelativeSeries(sortedMap);

        return ts;
    }

    /**
     *
     * @param key an article's URL
     * @param timeInterval time-intervals that are being used in the application
     * i.e., v_1m, v_1h, v_10s (each corresponds to cassandra supercolumn)
     * @param visitsLowerBound the minimum threshold value of the facebook
     * shares
     * @return
     */
    public TimeSeriesSorted getTimeSeriesColumnValues(String key, String timeInterval, Integer visitsLowerBound) {

        String superColumn = timeInterval;
        TimeSeriesSorted ts = new TimeSeriesSorted();
        ts.setKey(key);
        TreeMap<Long, Integer> seriesData = timeSeries.get(superColumn, key, visitsLowerBound);
        if (seriesData != null) {
            SortedMap<Long, Integer> sortedMap = seriesData.subMap(seriesData.firstKey(), seriesData.lastKey());
            ts.setRelativeSeries(sortedMap);
        } else {
        	throw new IllegalStateException("Got null time series for superColumn: '" + superColumn + "', key: '" + key + "', visitsLowerBound: " + visitsLowerBound);
        }
        return ts;
    }

    public void printArticleByVisits(String domain, String superColumn) {

        TreeMap<String, Integer> seriesData = timeSeries.getTopArticles(domain, superColumn);
        for (Map.Entry<String, Integer> entry : seriesData.entrySet()) {

            System.out.println(entry.getValue() + " - " + entry.getKey());
        }
    }

    /**
     * This method is not yet completed.
     *
     * @param timeSeriesList
     * @param numberofWeeks
     * @return
     */
    public List<TimeSeriesSorted> getXWeeksSeries(List<TimeSeriesSorted> timeSeriesList, int numberofWeeks) {
        List<TimeSeriesSorted> timeSeriesListRSP = new ArrayList<TimeSeriesSorted>();
        Iterator tsITR = timeSeriesList.iterator();
        int points = 0;
        long startofWeek = 0;
        while (tsITR.hasNext()) {
            TimeSeriesSorted tempSeries = new TimeSeriesSorted();
            TimeSeriesSorted ts = (TimeSeriesSorted) tsITR.next();
            TreeMap<Long, Integer> tempMap = new TreeMap<Long, Integer>();
            TreeMap<Long, Integer> mainMap = new TreeMap<Long, Integer>();
            boolean weekFound = false;
            SortedMap<Long, Integer> map = ts.getRelativeSeries();
            for (Map.Entry<Long, Integer> entry : map.entrySet()) {

                long nextPoint = entry.getKey();
                if (points == 0) { // considering first point of the series as start of a week
                    startofWeek = entry.getKey();
                    tempMap.put(startofWeek, entry.getValue());
                } else {
                    tempMap.put(nextPoint, entry.getValue()); // filling tempMap with key and value/visits
                }

                if (isXdaysDiff(startofWeek, nextPoint, 7)) { //checking if the diff b/w the start and next point is of 7 days 
                    points = 0; //Initialize the points of a week to zero
                    mainMap.putAll(tempMap); //merging tempMap into mainMap
                    tempMap.clear();
                    weekFound = true;
                } else {
                    points++;
                }
            }

            if (weekFound) {
                tempSeries.setKey(ts.getKey());
                tempSeries.setRelativeSeries(mainMap);
                timeSeriesListRSP.add(tempSeries);
            }

        }
        return timeSeriesListRSP;
    }

    /**
     * Not yet implemented.
     *
     * @param inputSeries
     * @param timeInterval
     * @param timeFormat
     * @return
     */
    public SortedMap<Long, Integer> getSampleSeries(SortedMap<Long, Integer> inputSeries, int timeInterval, String timeFormat) {

        throw new UnsupportedOperationException("Not yet implemented");
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

    private boolean isXdaysDiff(Long startTime, Long endTime, int days) {
        long diffTime = endTime - startTime;
        long diffDays = diffTime / (1000 * 60 * 60 * 24);
        //System.out.println("Diff in Days" + diffDays);
        if (diffDays >= days) {
            return true;
        } else {
            return false;
        }
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
            //  if (entry.getValue() > 0) {  // I'm not sure about his condition. It consider only non-zero values as points for taking average.
            totalPoints++;
            //}
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

    public Double getAllSeriesAverage(List<TimeSeriesSorted> timeSeriesList) {

        for (TimeSeriesSorted ts : timeSeriesList) {
            SortedMap<Long, Integer> map = ts.getRelativeSeries();
            DateTime t1 = new DateTime(map.firstKey());
            DateTime t2 = new DateTime(map.lastKey());
            System.out.println("Start of series "+ t1.toString());
            System.out.println("End of series "+ t2.toString());
            Weeks w = Weeks.weeksBetween(new DateTime(map.firstKey()), new DateTime(map.lastKey()));
            System.out.println("Weeks Between: "+ w.getWeeks());
        }
        return 0d;
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
            SortedMap<Long, Float> map2 = ts.getNormalizedSeries();
            for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                System.out.println("Time - Rel-Visits - Nor-Visits: " + entry.getKey() + " - " + entry.getValue() + " - " + Math.round(map2.get(entry.getKey())));
            }
        }
    }

    /**
     *
     * @param sortedTS
     */
    public void drawLineChart(List<TimeSeriesSorted> sortedTS) {

        DrawTimeSeriesLineCharts draw = new DrawTimeSeriesLineCharts("Article Visits: Line Chart");
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
        movAvgChart.drawTimeSeriesMovAvgChart(sortedTS, movAvgPoints, "Article Visits: Moving Avg.", "Data & Time", "Visits", legend);
    }

    /**
     *
     * @param sortedTS
     * @param seriesType
     * @param legend
     */
    public void drawTSChart(List<TimeSeriesSorted> sortedTS, String seriesType, boolean legend) {

        DrawTimeSeriesChart demo = new DrawTimeSeriesChart("Time Series Chart");
        demo.drawTimeSeriesURLVisitTSChart(sortedTS, seriesType, "Article Visits: Time Series", "Data & Time", "Visits", legend);

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

    public void filterArticles1(String domain, String superColumn) {

        TreeMap<String, Integer> seriesData = timeSeries.filterArticles(domain, superColumn);
        System.out.println("Selected articles: " + seriesData.size());
        for (Map.Entry<String, Integer> entry : seriesData.entrySet()) {
            System.out.println(entry.getValue() + " - " + entry.getKey());
        }
    }
    
    public void filterArticles2(String domain, String superColumn) {

        TreeMap<String, Integer> seriesData = timeSeries.filterArticles2(domain, superColumn);
        System.out.println("Selected articles: " + seriesData.size()); 
        for (Map.Entry<String, Integer> entry : seriesData.entrySet()) {
            System.out.println(entry.getValue() + " - " + entry.getKey());
        }
    }

    /**
     *
     * @param argus
     */
    public static void main(String argus[]) {


        VisitsAnalysis analysisDriver = new VisitsAnalysis();
        analysisDriver.getTimeSeriesKeyspace();
        
        analysisDriver.filterArticles2(StaticVars.ALJAZEERA_DOMAIN, TimeSeriesIntervals.KEY_ONE_MINUTE);
        
//        String key = "http://www.aljazeera.com/indepth/opinion/2011/08/201182511546451332.html";
//        TimeSeriesSorted ts = analysisDriver.getTimeSeriesColumnValues(key,TimeSeriesIntervals.KEY_ONE_MINUTE, 0);
//        List<TimeSeriesSorted> tsList = new ArrayList<TimeSeriesSorted>();
//        tsList.add(ts);
//        
//        analysisDriver.getAllSeriesAverage(tsList);



    }
}
