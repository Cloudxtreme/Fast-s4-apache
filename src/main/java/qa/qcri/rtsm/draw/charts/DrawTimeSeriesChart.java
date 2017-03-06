/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.draw.charts;

import java.awt.BorderLayout;
import java.awt.Color;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import javax.swing.JFrame;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import qa.qcri.rtsm.analysis.imran.TimeSeriesSorted;
import qa.qcri.rtsm.ui.StaticVars;

/**
 *
 * @author Imran
 */
public class DrawTimeSeriesChart extends ApplicationFrame {

    public DrawTimeSeriesChart(String title) {
        super(title);
    }

    public void drawTimeSeriesURLVisitTSChart(List<TimeSeriesSorted> timeSeriesList, String seriesType, String title, String xLabel, String yLabel, boolean legend) {

        //final XYDataset dataset = createTimeSeriesDataset(timeSeriesList, seriesType);
        final XYDataset dataset = createTimeSeriesDataset(timeSeriesList);
        final JFreeChart chart = createChart(dataset, title, xLabel, yLabel, legend);
        ChartPanel chartPanel = new ChartPanel(chart, false);
        chartPanel.setPreferredSize(new java.awt.Dimension(1200, 470));
        chartPanel.setMouseZoomable(true, false);
        //setContentPane(chartPanel);

        JFrame f = new JFrame("");
        f.setTitle("");
        f.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        f.setLayout(new BorderLayout(0, 5));
        f.add(chartPanel, BorderLayout.CENTER);
        f.pack();
        f.setLocationRelativeTo(null);
        f.setVisible(true);

    }

    /**
     * Creates a chart.
     *
     * @param dataset a dataset.
     *
     * @return A chart.
     */
    private static JFreeChart createChart(XYDataset dataset, String title, String xLabel, String yLabel, boolean legend) {

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                title, // title
                xLabel, // x-axis label
                yLabel, // y-axis label
                dataset, // data
                legend, // create legend?
                true, // generate tooltips?
                false // generate URLs?
                );

        chart.setBackgroundPaint(Color.white);

        XYPlot plot = (XYPlot) chart.getPlot();
//        plot.setBackgroundPaint(Color.lightGray);
//        plot.setDomainGridlinePaint(Color.white);
//        plot.setRangeGridlinePaint(Color.white);
//        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
//        plot.setDomainCrosshairVisible(true);
//        plot.setRangeCrosshairVisible(true);
//        
//        XYItemRenderer r = plot.getRenderer();
//        if (r instanceof XYLineAndShapeRenderer) {
//            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
//            renderer.setBaseShapesVisible(true);
//            renderer.setBaseShapesFilled(true);
//            
//            //renderer.setDefaultShapesVisible(true);
//            //renderer.setDefaultShapesFilled(true);
//        }
//        
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("dd-MMM:hh-mm"));


        final XYItemRenderer renderer = chart.getXYPlot().getRenderer();
        final StandardXYToolTipGenerator g = new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("d-MMM:hh:mm:ss"), new DecimalFormat("0.00"));
        renderer.setToolTipGenerator(g);
        return chart;

        //return chart;

    }

    @Deprecated
    private XYDataset createTimeSeriesDataset(List<TimeSeriesSorted> timeSeriesList, String seriesType) {
        final TimeSeriesCollection dataset = new TimeSeriesCollection();

        Iterator itr = timeSeriesList.iterator();
        List<TimeSeries> chartSeries = new ArrayList<TimeSeries>();
        while (itr.hasNext()) {


            TimeSeriesSorted timeSeries = (TimeSeriesSorted) itr.next();
            String title ="";
            if (timeSeries.getAlias() != null){
                title = timeSeries.getAlias();
            }
            else {
                title = timeSeries.getKey();
            }
            TimeSeries series = new TimeSeries(title);
            SortedMap<Long, Integer> map = null;
            SortedMap<Long, Float> map2 = null;
            if (seriesType.equals(StaticVars.SERIES_TYPE_RELATIVE)) {
                map = timeSeries.getRelativeSeries();
                for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                    series.add(new Millisecond(new Date(entry.getKey())), entry.getValue());
                }

            }
            if (seriesType.equals(StaticVars.SERIES_TYPE_ABSOLUTE)) {
                map = timeSeries.getAbsoluteSeries();
                for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                    series.add(new Millisecond(new Date(entry.getKey())), entry.getValue());
                }

            }
            if (seriesType.equals(StaticVars.SERIES_TYPE_NORMALIZED)) {
                map2 = timeSeries.getNormalizedSeries();
                for (Map.Entry<Long, Float> entry : map2.entrySet()) {
                    series.add(new Millisecond(new Date(entry.getKey())), entry.getValue());
                }

            }
            chartSeries.add(series);
        }

        for (int i = 0; i < chartSeries.size(); i++) {
            dataset.addSeries(chartSeries.get(i));
        }
        return dataset;
    }
    
    private XYDataset createTimeSeriesDataset(List<TimeSeriesSorted> timeSeriesList) {
        final TimeSeriesCollection dataset = new TimeSeriesCollection();

        Iterator itr = timeSeriesList.iterator();
        List<TimeSeries> chartSeries = new ArrayList<TimeSeries>();
        while (itr.hasNext()) {


            TimeSeriesSorted timeSeries = (TimeSeriesSorted) itr.next();
            String title ="";
            if (timeSeries.getAlias() != null){
                title = timeSeries.getAlias();
            }
            else {
                title = timeSeries.getKey();
            }
            TimeSeries series = new TimeSeries(title);
            SortedMap<Long, Integer> map = null;
            SortedMap<Long, Float> map2 = null;
            if (timeSeries.getChartSeries().equals(StaticVars.SERIES_TYPE_RELATIVE)) {
                map = timeSeries.getRelativeSeries();
                for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                    series.add(new Millisecond(new Date(entry.getKey())), entry.getValue());
                }

            }
            if (timeSeries.getChartSeries().equals(StaticVars.SERIES_TYPE_ABSOLUTE)) {
                map = timeSeries.getAbsoluteSeries();
                for (Map.Entry<Long, Integer> entry : map.entrySet()) {
                    series.add(new Millisecond(new Date(entry.getKey())), entry.getValue());
                }

            }
            if (timeSeries.getChartSeries().equals(StaticVars.SERIES_TYPE_NORMALIZED)) {
                map2 = timeSeries.getNormalizedSeries();
                for (Map.Entry<Long, Float> entry : map2.entrySet()) {
                    series.add(new Millisecond(new Date(entry.getKey())), entry.getValue());
                }

            }
            chartSeries.add(series);
        }

        for (int i = 0; i < chartSeries.size(); i++) {
            dataset.addSeries(chartSeries.get(i));
        }
        return dataset;
    }
    

}
