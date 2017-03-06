/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.analysis.imran;

import java.util.SortedMap;

/**
 *
 * @author Imran
 * @deprecated Use {@link TimeSeries}
 */
public class TimeSeriesSorted {

    private String key;
    private String alias;
    private SortedMap<Long, Integer> relativeSeries;
    private SortedMap<Long, Integer> absoluteSeries;
    private SortedMap<Long, Float> normalizedSeries;
    private float averagePoint;
    private String chartSeries;

    public TimeSeriesSorted() {
    }

    public TimeSeriesSorted(String key) {
        this.key = key;

    }

    /**
     * @return the key
     */
    public String getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return the map
     */
    public SortedMap<Long, Integer> getRelativeSeries() {
        return relativeSeries;
    }

    /**
     * @param map the map to set
     */
    public void setRelativeSeries(SortedMap<Long, Integer> map) {
        this.relativeSeries = map;
    }

    /**
     * @return the averagePoint
     */
    public float getAveragePoint() {
        return averagePoint;
    }

    /**
     * @param averagePoint the averagePoint to set
     */
    public void setAveragePoint(float averagePoint) {
        this.averagePoint = averagePoint;
    }

    /**
     * @return the absoluteMap
     */
    public SortedMap<Long, Integer> getAbsoluteSeries() {
        return absoluteSeries;
    }

    /**
     * @param absoluteMap the absoluteMap to set
     */
    public void setAbsoluteSeries(SortedMap<Long, Integer> absoluteMap) {
        this.absoluteSeries = absoluteMap;
    }

    /**
     * @return the normalizedSeries
     */
    public SortedMap<Long, Float> getNormalizedSeries() {
        return normalizedSeries;
    }

    /**
     * @param normalizedSeries the normalizedSeries to set
     */
    public void setNormalizedSeries(SortedMap<Long, Float> normalizedSeries) {
        this.normalizedSeries = normalizedSeries;
    }

    /**
     * @return the alias
     */
    public String getAlias() {
        return alias;
    }

    /**
     * @param alias the alias to set
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    /**
     * @return the chartSeries
     */
    public String getChartSeries() {
        return chartSeries;
    }

    /**
     * @param chartSeries the chartSeries to set
     */
    public void setChartSeries(String chartSeries) {
        this.chartSeries = chartSeries;
    }
}
