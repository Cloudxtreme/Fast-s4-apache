/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.draw.charts;

import java.awt.BorderLayout;
import java.awt.Color;
import java.util.*;
import javax.swing.JFrame;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import qa.qcri.rtsm.analysis.imran.TimeSeriesSorted;

/**
 *
 * @author Imran
 */
public class DrawTimeSeriesLineCharts {

    public DrawTimeSeriesLineCharts(String title) {
        //super(title);
    }

    public void drawTimeSeriesURLVisitLineChart(List<TimeSeriesSorted> timeSeriesList) {

        final XYDataset dataset = createTimeSeriesDataset(timeSeriesList);
        final JFreeChart chart = createChart(dataset);
        final ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(1200, 470));
        JFrame f = new JFrame("");
        f.setTitle("");
        f.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        f.setLayout(new BorderLayout(0, 5));
        f.add(chartPanel, BorderLayout.CENTER);
        f.pack();
        f.setLocationRelativeTo(null);
        f.setVisible(true);
       // setContentPane(chartPanel);

    }

    private XYDataset createTimeSeriesDataset(List<TimeSeriesSorted> timeSeriesList) {
        final XYSeriesCollection dataset = new XYSeriesCollection();

        Iterator itr = timeSeriesList.iterator();
        List<XYSeries> chartSeries = new ArrayList<XYSeries>();
        while (itr.hasNext()) {

            TimeSeriesSorted timeSeries = (TimeSeriesSorted) itr.next();
            SortedMap<Long, Integer> map = timeSeries.getRelativeSeries();
            String key = timeSeries.getKey();
            XYSeries series = new XYSeries(key);

            Collection c = map.keySet();
            Iterator itr2 = c.iterator();
            while (itr2.hasNext()) {

                Long longTime = (Long) itr2.next();
                Integer value = map.get(longTime);
                series.add(longTime, value);

            }

            chartSeries.add(series);
        }


        for (int i = 0; i < chartSeries.size(); i++) {

            dataset.addSeries(chartSeries.get(i));
        }



        return dataset;
    }

    private JFreeChart createChart(final XYDataset dataset) {

        // create the chart...
        final JFreeChart chart = ChartFactory.createXYLineChart(
                "URL Visits", // chart title
                "Time", // x axis label
                "Visits", // y axis label
                dataset, // data
                PlotOrientation.VERTICAL,
                false, // include legend
                true, // tooltips
                true // urls
                );

        // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
        chart.setBackgroundPaint(Color.white);

        //final StandardLegend legend = (StandardLegend) chart.getLegend();
        //legend.setDisplaySeriesShapes(true);

        // get a reference to the plot for further customisation...
        final XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.lightGray);
        //plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);

        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesLinesVisible(0, false);
        renderer.setSeriesShapesVisible(1, false);
        plot.setRenderer(renderer);

        // change the auto tick unit selection to integer units only...
        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        //plot.getRangeAxis().setRange(0l, 20l);
        // OPTIONAL CUSTOMISATION COMPLETED.



        return chart;

    }
}
