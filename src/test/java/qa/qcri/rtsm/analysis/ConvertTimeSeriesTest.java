package qa.qcri.rtsm.analysis;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.analysis.ConvertTimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries.Point;


public class ConvertTimeSeriesTest {

	private ConvertTimeSeries cts;
	
	@Before
	public void setUp() throws Exception {
		
	}
	
	@Test
	public void generateSumSeries() throws IOException {
		
		TimeSeries tsa = new TimeSeries("a");
		tsa.addPoint(new Point(new Long(1), new Double(1)));
		tsa.addPoint(new Point(new Long(2), new Double(2)));	
		tsa.addPoint(new Point(new Long(3), new Double(3)));
		tsa.addPoint(new Point(new Long(4), new Double(4)));
		
		TimeSeries tsb = new TimeSeries("b");
		tsb.addPoint(new Point(new Long(1), new Double(10)));
		tsb.addPoint(new Point(new Long(2), new Double(9)));	
		tsb.addPoint(new Point(new Long(3), new Double(8)));
		tsb.addPoint(new Point(new Long(4), new Double(7)));
		
		TimeSeries[] aSeries = {tsa, tsb};
		cts = new ConvertTimeSeries(aSeries);
		TimeSeries ts = cts.generateSumSeries();	
		assertEquals(44, ts.sumValues());
	}
	
	@Test
	public void generateCumulativeEventSeries() {
		TimeSeries tsa = new TimeSeries("a");
		tsa.addPoint(new Point(new Long(1), new Double(1)));
		tsa.addPoint(new Point(new Long(2), new Double(2)));	
		tsa.addPoint(new Point(new Long(3), new Double(3)));
		tsa.addPoint(new Point(new Long(4), new Double(4)));
		
		TimeSeries tsb = new TimeSeries("b");
		tsb.addPoint(new Point(new Long(1), new Double(10)));
		tsb.addPoint(new Point(new Long(2), new Double(9)));	
		tsb.addPoint(new Point(new Long(3), new Double(8)));
		tsb.addPoint(new Point(new Long(4), new Double(7)));
		
		TimeSeries[] aSeries = {tsa, tsb};
		cts = new ConvertTimeSeries(aSeries);
		TimeSeries ts = cts.generateCumulativeEventSeries();	
		assertEquals(66, ts.sumValues());
	}

}