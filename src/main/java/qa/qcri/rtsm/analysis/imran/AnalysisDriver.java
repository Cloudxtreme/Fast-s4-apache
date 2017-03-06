/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.analysis.imran;

import java.util.ArrayList;
import java.util.List;

import qa.qcri.rtsm.draw.charts.DrawMovingAvgChart;
import qa.qcri.rtsm.draw.charts.DrawTimeSeriesLineCharts;
import qa.qcri.rtsm.draw.charts.DrawTimeSeriesChart;
import qa.qcri.rtsm.ui.StaticVars;

/**
 *
 * @author Imran
 */
public class AnalysisDriver {
    
   private VisitsAnalysis visitAnalysis;
   private TweetsAnalysis tweetsAnalysis;
   private FacebookShareAnalysis FBShareAnalysis;

    /**
     * 
     */
    public AnalysisDriver() {
        visitAnalysis = new VisitsAnalysis();
        tweetsAnalysis = new TweetsAnalysis();
        FBShareAnalysis = new FacebookShareAnalysis();
    }
    
    /**
     * 
     * @param args
     */
    public static void main(String[] args) {
        
        AnalysisDriver analysis= new AnalysisDriver();
        analysis.visitAnalysis.getTimeSeriesKeyspace();
        analysis.FBShareAnalysis.getTimeSeriesKeyspace();
        analysis.tweetsAnalysis.getTweetsKeyspace();
        
        //String key="http://www.aljazeera.com/indepth/opinion/2012/09/201295614181450.html";
        String key="http://www.aljazeera.com/news/middleeast/2012/09/2012911183011369379.html";
        List<TimeSeriesSorted> tsList = new ArrayList<TimeSeriesSorted>();
        TimeSeriesSorted visitTS = analysis.visitAnalysis.getTimeSeriesColumnValues(key, TimeSeriesIntervals.KEY_ONE_MINUTE, 0);
        visitTS = analysis.visitAnalysis.getAveNormalizedSeries(visitTS);
        visitTS.setAlias("Visits");
        visitTS.setChartSeries(StaticVars.SERIES_TYPE_NORMALIZED);
        
        TimeSeriesSorted FBTS = analysis.FBShareAnalysis.getTimeSeriesColumnValues(key, TimeSeriesIntervals.KEY_ONE_MINUTE_FB, 0);
        FBTS = analysis.FBShareAnalysis.constructRelativeSeries2(FBTS, 1, StaticVars.TIME_FORMAT_MIN);
        FBTS.setAlias("FacebookShares");
        FBTS.setChartSeries(StaticVars.SERIES_TYPE_RELATIVE);

        TimeSeriesSorted tweetsTS = analysis.tweetsAnalysis.constructAccumulativeTweetsTS(key);
        tweetsTS = analysis.tweetsAnalysis.constructRelativeSeries(tweetsTS, 1, StaticVars.TIME_FORMAT_MIN);
        tweetsTS.setAlias("Tweets");
        tweetsTS.setChartSeries(StaticVars.SERIES_TYPE_RELATIVE);
        
        tsList.add(visitTS);
        tsList.add(FBTS);
        tsList.add(tweetsTS);
        
        //analysis.visitAnalysis.drawTSChart(tsList, StaticVars.SERIES_TYPE_RELATIVE, true);
        analysis.drawMovAvgChart(tsList, 20, true);
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
        movAvgChart.drawTimeSeriesMovAvgChart(sortedTS, movAvgPoints, "Moving Avg.", "Data & Time", "Visits, FB Shares, Tweets", legend);
    }

    /**
     * 
     * @param sortedTS
     * @param seriesType
     */
    public void drawTSChart(List<TimeSeriesSorted> sortedTS, String seriesType) {

        DrawTimeSeriesChart demo = new DrawTimeSeriesChart("");
        demo.drawTimeSeriesURLVisitTSChart(sortedTS, seriesType, "Time Series", "Data & Time", "Visits, FB Shares, Tweets", false);

    }

    
}
