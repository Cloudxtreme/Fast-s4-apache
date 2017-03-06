/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.analysis.imran;

import java.util.*;


/**
 *
 * @author Imran
 */
public class AccumulativeTimeSeries {

    /**
     *
     * @param timeSeriesList
     * @param numberofWeeks
     * @return
     */
    public TimeSeriesSorted getAccumulativeTimeSeries(List<TimeSeriesSorted> timeSeriesList, int numberofWeeks) {
        TimeSeriesSorted accumulativeSeries = new TimeSeriesSorted();
        Iterator tsITR = timeSeriesList.iterator();
        TreeMap<Long, Integer> mainMap = new TreeMap<Long, Integer>();
        while (tsITR.hasNext()) {
            TimeSeriesSorted ts = (TimeSeriesSorted) tsITR.next();
            SortedMap<Long, Integer> map = ts.getRelativeSeries();
            mainMap = getAccumulateMap(mainMap, map);
        }

        accumulativeSeries.setRelativeSeries(mainMap);
        accumulativeSeries.setKey("Accumulative Series");
        accumulativeSeries.setAveragePoint(0);
        return accumulativeSeries;
    }

    private TreeMap<Long, Integer> getAccumulateMap(TreeMap<Long, Integer> mainMap, SortedMap<Long, Integer> map) {

        for (Map.Entry<Long, Integer> entry : map.entrySet()) {
            if (mainMap.containsKey(entry.getKey())) {
                Integer value = mainMap.get(entry.getKey());
                mainMap.put(entry.getKey(), value + entry.getValue());
            } else {
                mainMap.put(entry.getKey(), entry.getValue());
            }
        }

        return mainMap;
    }

    /**
     *
     * @param ts1
     * @param ts2
     * @param ts3
     * @return
     */
    public List<TimeSeriesSorted> getEqualTimeSeries(TimeSeriesSorted ts1, TimeSeriesSorted ts2, TimeSeriesSorted ts3) {

        throw new UnsupportedOperationException("Not yet implemented");
    }

    private TimeSeriesSorted getSmallestTS(TimeSeriesSorted ts1, TimeSeriesSorted ts2, TimeSeriesSorted ts3) {

        if (ts1.getRelativeSeries().size() > ts2.getRelativeSeries().size() && ts1.getRelativeSeries().size() > ts3.getRelativeSeries().size()) {
            return ts1;
        } else if (ts2.getRelativeSeries().size() > ts1.getRelativeSeries().size() && ts2.getRelativeSeries().size() > ts3.getRelativeSeries().size()) {
            return ts2;
        } else if (ts3.getRelativeSeries().size() > ts1.getRelativeSeries().size() && ts3.getRelativeSeries().size() > ts2.getRelativeSeries().size()) {
            return ts3;
        }

        return ts1;
    }
}
