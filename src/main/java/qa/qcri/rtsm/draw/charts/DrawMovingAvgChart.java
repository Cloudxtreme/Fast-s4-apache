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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.MovingAverage;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;

import qa.qcri.rtsm.analysis.imran.TimeSeriesSorted;
import qa.qcri.rtsm.ui.StaticVars;

/**
 *
 * @author Imran
 */
public class DrawMovingAvgChart {

    public void drawTimeSeriesMovAvgChart(List<TimeSeriesSorted> timeSeriesList, Integer movAvgPointsTxtB, String title, String xLabel, String yLabel, boolean legend) {

        final XYDataset dataset = createTimeSeriesDataset(timeSeriesList, movAvgPointsTxtB);
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

        final XYItemRenderer renderer = chart.getXYPlot().getRenderer();
        final XYPlot plot = chart.getXYPlot();
        final StandardXYToolTipGenerator g = new StandardXYToolTipGenerator(
                StandardXYToolTipGenerator.DEFAULT_TOOL_TIP_FORMAT,
                new SimpleDateFormat("dd-MMM-yyyy:hh:mm:ss"), new DecimalFormat("0.00"));
        renderer.setToolTipGenerator(g);
        //plot.getRangeAxis().setRange(0, 3);
        return chart;

        //return chart;

    }

    private XYDataset createTimeSeriesDataset(List<TimeSeriesSorted> timeSeriesList, Integer movAvgPointsTxtB) {
        final TimeSeriesCollection dataset = new TimeSeriesCollection();

        Iterator itr = timeSeriesList.iterator();
        List<TimeSeries> chartSeries = new ArrayList<TimeSeries>();
        while (itr.hasNext()) {
            TimeSeriesSorted timeSeries = (TimeSeriesSorted) itr.next();
            String title = "";
            if (timeSeries.getAlias() != null) {
                title = timeSeries.getAlias();
            } else {
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
            TimeSeries seriesMovAvg = MovingAverage.createPointMovingAverage(series, title, movAvgPointsTxtB);
            chartSeries.add(seriesMovAvg);
        }

        for (int i = 0; i < chartSeries.size(); i++) {
            dataset.addSeries(chartSeries.get(i));
        }
        return dataset;
    }

    @Deprecated
    private XYDataset createDataset(List<TimeSeriesSorted> timeSeriesList, Integer movAvgPointsTxtB) {
        final TimeSeriesCollection dataset = new TimeSeriesCollection();

        Iterator itr = timeSeriesList.iterator();
        List<TimeSeries> chartSeries = new ArrayList<TimeSeries>();
        while (itr.hasNext()) {

            TimeSeriesSorted timeSeries = (TimeSeriesSorted) itr.next();
            SortedMap<Long, Integer> map = timeSeries.getRelativeSeries();
            String key = timeSeries.getKey();
            TimeSeries series = new TimeSeries(timeSeries.getAlias());

            Collection c = map.keySet();
            Iterator itr2 = c.iterator();
            while (itr2.hasNext()) {

                Long longTime = (Long) itr2.next();
                Date dt = new Date(longTime);
                Integer value = map.get(longTime);
                series.add(new Millisecond(dt), value);

            }
            String articleTitle = "";

            //getting title of the article from the key/url
            try {
                System.out.println("Getting title of the article:" + key);
                //articleTitle = WebUtil.getTitleOfParsedHTML(WebUtil.getParsedHTML(WebUtil.getHTMLContentsAsString(key)));

            } catch (Exception ex) {
                Logger.getLogger(DrawMovingAvgChart.class.getName()).log(Level.SEVERE, null, ex);
            }

            if (!timeSeries.getAlias().equals("")) {
                articleTitle = timeSeries.getAlias();
            }
            TimeSeries seriesMovAvg = MovingAverage.createPointMovingAverage(series, articleTitle, movAvgPointsTxtB);
            chartSeries.add(seriesMovAvg);
        }
        for (int i = 0; i < chartSeries.size(); i++) {
            dataset.addSeries(chartSeries.get(i));
        }
        return dataset;
    }
}
