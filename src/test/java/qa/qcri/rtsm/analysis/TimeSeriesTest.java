package qa.qcri.rtsm.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.analysis.TimeSeries.EmptySeriesException;
import qa.qcri.rtsm.analysis.TimeSeries.LineParser;
import qa.qcri.rtsm.analysis.TimeSeries.Point;

public class TimeSeriesTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testInsertPoint() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(1), new Double(13)));
		assertEquals(1, ts.size());
		try {
			ts.insertPoint(new Point(new Long(1), new Double(3)));
			fail("Should have thrown an exception");
		} catch (IllegalArgumentException e) {
			// ok
		}
		assertEquals(new Double(13), ts.get(new Long(1)));
	}

	@Test
	public void testAddPoint() {
		TimeSeries ts = new TimeSeries("a");
		ts.addPoint(new Point(new Long(1), new Double(13)));
		assertEquals(1, ts.size());
		ts.addPoint(new Point(new Long(1), new Double(7)));
		assertEquals(1, ts.size());
		ts.addPoint(new Point(new Long(2), new Double(10)));
		assertEquals(2, ts.size());
		assertEquals(new Double(20), ts.get(new Long(1)));
		assertEquals(new Double(10), ts.get(new Long(2)));
	}

	@Test
	public void testTimeSeriesListLineParserString() throws EmptySeriesException {
		Vector<String> lines = new Vector<String>();
		lines.add("1	1000");
		lines.add("2	1100");
		lines.add("3	2000");
		lines.add("4	3120");
		TimeSeries ts = new TimeSeries(lines, new LineParser(), "a");
		assertEquals(4, ts.size());
		assertEquals(new Double(1000), ts.get(new Long(1)));
		assertEquals(new Double(1100), ts.get(new Long(2)));
		assertEquals(new Double(2000), ts.get(new Long(3)));
		assertEquals(new Double(3120), ts.get(new Long(4)));
		
		// Test exception
		lines.clear();
		lines.add("1	1000");
		lines.add("2	1100");
		lines.add("2	2000");
		try {
			ts = new TimeSeries( lines, new LineParser(), "a" );
			fail("Should have thrown an exception");
		} catch( IllegalArgumentException e ) {
			// ok
		}
	}

	@Test
	public void testGetArrayList() throws IOException {
		Vector<String> lines = new Vector<String>();
		lines.add("1	1000	1004");
		lines.add("2	1100	1103");
		lines.add("3	2000	2002");
		lines.add("4	3120	3121");
		ArrayList<TimeSeries> tss = TimeSeries.getArrayList(lines, new LineParser(), new String[] { "a", "b" }, 2 );
		assertEquals( 2, tss.size() );
		
		TimeSeries ts1 = tss.get(0);
		TimeSeries ts2 = tss.get(1);
		assertEquals( "a", ts1.getLabel() );
		assertEquals( "b", ts2.getLabel() );

		assertEquals( 4, ts1.size() );
		assertEquals( new Double(1000), ts1.get(new Long(1)));
		assertEquals( new Double(1100), ts1.get(new Long(2)));
		assertEquals( new Double(2000), ts1.get(new Long(3)));
		assertEquals( new Double(3120), ts1.get(new Long(4)));
		
		assertEquals( 4, ts2.size() );
		assertEquals( new Double(1004), ts2.get(new Long(1)));
		assertEquals( new Double(1103), ts2.get(new Long(2)));
		assertEquals( new Double(2002), ts2.get(new Long(3)));
		assertEquals( new Double(3121), ts2.get(new Long(4)));
	}

	@Test
	public void testGetLines() {
		TimeSeries ts = new TimeSeries("a");
		ts.addPoint(new Point(new Long(1), new Double(13)));
		ts.addPoint(new Point(new Long(1), new Double(7)));
		ts.addPoint(new Point(new Long(2), new Double(10)));
		ArrayList<String> lines = ts.getLines();
		assertEquals(2, lines.size());
		assertEquals("1	20.0", lines.get(0));
		assertEquals("2	10.0", lines.get(1));
	}

	@Test
	public void testPointWiseAddition() {
		TimeSeries ts1 = new TimeSeries("a");
		ts1.insertPoint(new Point(new Long(1), new Double(13)));
		ts1.insertPoint(new Point(new Long(2), new Double(7)));
		ts1.insertPoint(new Point(new Long(3), new Double(11)));
		
		TimeSeries ts2 = new TimeSeries("b");
		ts2.insertPoint(new Point(new Long(1), new Double(4)));
		ts2.insertPoint(new Point(new Long(2), new Double(8)));
		ts2.insertPoint(new Point(new Long(4), new Double(21)));
		ts2.insertPoint(new Point(new Long(5), new Double(33)));
		
		assertEquals(3, ts1.size());

		ts1.pointWiseAddition(ts2);
		
		assertEquals(5, ts1.size());
		assertEquals(4, ts2.size());
		
		assertEquals( new Double(17), ts1.get(new Long(1)));
		assertEquals( new Double(15), ts1.get(new Long(2)));
		assertEquals( new Double(11), ts1.get(new Long(3)));
		assertEquals( new Double(21), ts1.get(new Long(4)));
		assertEquals( new Double(33), ts1.get(new Long(5)));
	}

	@Test
	public void testFirstPoint() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(3), new Double(11)));
		ts.insertPoint(new Point(new Long(1), new Double(13)));
		ts.insertPoint(new Point(new Long(2), new Double(7)));
	
		assertEquals( new Point(new Long(1), new Double(13)), ts.firstPoint());
	}

	@Test
	public void testGetDates() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(3), new Double(11)));
		ts.insertPoint(new Point(new Long(1), new Double(13)));
		ts.insertPoint(new Point(new Long(2), new Double(7)));
		Set<Long> dates = ts.getDates();
		assertEquals( 3, dates.size() );
		assertTrue( dates.contains(new Long(1)));
		assertTrue( dates.contains(new Long(2)));
		assertTrue( dates.contains(new Long(3)));
	}

	@Test
	public void testBeginsBefore() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(11)));
		ts.insertPoint(new Point(new Long(20), new Double(13)));
		ts.insertPoint(new Point(new Long(30), new Double(7)));
		
		assertTrue( ts.beginsBefore( 11l ) );
		assertFalse( ts.beginsBefore( 9l ) );
	}

	@Test
	public void testBeginsAfter() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(11)));
		ts.insertPoint(new Point(new Long(20), new Double(13)));
		ts.insertPoint(new Point(new Long(30), new Double(7)));
		
		assertTrue( ts.beginsAfter( 9l ) );
		assertFalse( ts.beginsAfter( 11l ) );
	}

	@Test
	public void testCountEventsBefore() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(11)));
		ts.insertPoint(new Point(new Long(20), new Double(13)));
		ts.insertPoint(new Point(new Long(30), new Double(7)));
		
		assertEquals( 31, ts.countEventsBefore(new Long(35)));
		assertEquals( 24, ts.countEventsBefore(new Long(25)));
		assertEquals( 11, ts.countEventsBefore(new Long(15)));
		assertEquals( 0, ts.countEventsBefore(new Long(5)));
	}
	
	@Test
	public void testCountEventsBetween() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(11)));
		ts.insertPoint(new Point(new Long(20), new Double(5)));
		ts.insertPoint(new Point(new Long(30), new Double(13)));
		ts.insertPoint(new Point(new Long(40), new Double(7)));
		
		assertEquals( 11, ts.countEventsBetween(new Long(5),new Long(15)));
		assertEquals( 5, ts.countEventsBetween(new Long(15),new Long(25)));
		assertEquals( 13, ts.countEventsBetween(new Long(25),new Long(35)));
		assertEquals( 7, ts.countEventsBetween(new Long(35),new Long(45)));
		
		assertEquals( 18, ts.countEventsBetween(new Long(15),new Long(35)));
		
		assertEquals( 18, ts.countEventsBetween(new Long(15),new Long(40))); // inclusive, exclusive
		assertEquals( 29, ts.countEventsBetween(new Long(10),new Long(40))); // inclusive, exclusive
		
		
		
		
	}

	@Test
	public void testIterator() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(11)));
		ts.insertPoint(new Point(new Long(20), new Double(13)));
		ts.insertPoint(new Point(new Long(30), new Double(7)));
		
		int cnt = 0;
		for( Point p: ts ) {
			if( p.getKey().equals(new Long(10))) {
				assertEquals( new Double(11), p.getValue() );
			} else if( p.getKey().equals(new Long(20))) {
				assertEquals( new Double(13), p.getValue() );
			} else if( p.getKey().equals(new Long(30))) {
				assertEquals( new Double(7), p.getValue() );
			}
			cnt++;
		}
		assertEquals(3, cnt);
	}

	@Test
	public void testConvertTimeToNumberOfEventsGivenCumulative() {
		TimeSeries cum = new TimeSeries("cum");
		cum.insertPoint(new Point(new Long(10), new Double(11)));
		cum.insertPoint(new Point(new Long(20), new Double(24)));
		cum.insertPoint(new Point(new Long(30), new Double(31)));
		
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(6), new Double(1)));
		ts.insertPoint(new Point(new Long(14), new Double(2)));
		ts.insertPoint(new Point(new Long(18), new Double(3)));
		ts.insertPoint(new Point(new Long(21), new Double(4)));
		ts.insertPoint(new Point(new Long(34), new Double(5)));
		
		TimeSeries conv = ts.convertTimeToNumberOfEventsGivenCumulative(cum, false);
		assertEquals( 4, conv.size() );
		assertEquals( new Double(1), conv.get(new Long(0)));
		assertEquals( new Double(5), conv.get(new Long(11)));
		assertEquals( new Double(4), conv.get(new Long(24)));
		assertEquals( new Double(5), conv.get(new Long(31)));
	}

	@Test
	public void testGet() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(6), new Double(1)));
		ts.insertPoint(new Point(new Long(14), new Double(2)));
		ts.insertPoint(new Point(new Long(18), new Double(3)));
		
		assertEquals( null, ts.get(new Long(7)) );
		assertEquals( new Double(2), ts.get(new Long(14)) );
	}

	@Test
	public void testContainsKey() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(6), new Double(1)));
		ts.insertPoint(new Point(new Long(14), new Double(2)));
		ts.insertPoint(new Point(new Long(18), new Double(3)));
		
		assertTrue( ts.containsKey(new Long(6)));
		assertFalse( ts.containsKey(new Long(19)));
	}

	@Test
	public void testSumValues() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(11)));
		ts.insertPoint(new Point(new Long(20), new Double(24)));
		ts.insertPoint(new Point(new Long(30), new Double(31)));
		
		assertEquals( 66, ts.sumValues() );
	}

	@Test
	public void testMovingAverage() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(12)));
		ts.insertPoint(new Point(new Long(20), new Double(24)));
		ts.insertPoint(new Point(new Long(30), new Double(32)));
		
		TimeSeries mov = ts.movingAverage(20,5);
		assertEquals( 3, mov.size() );
		assertEquals( new Double(12), mov.get(new Long(10)));
		assertEquals( new Double((12+0+24)/3.0), mov.get(new Long(20)));
		assertEquals( new Double((0+24+0+32)/4.0), mov.get(new Long(30)));
	}

	@Test
	public void testRelativeSeriesIgnoreDescending() {
		TimeSeries ts = new TimeSeries("a");
		ts.insertPoint(new Point(new Long(10), new Double(12)));
		ts.insertPoint(new Point(new Long(20), new Double(24)));
		ts.insertPoint(new Point(new Long(30), new Double(32)));
		ts.insertPoint(new Point(new Long(40), new Double(30)));
		ts.insertPoint(new Point(new Long(50), new Double(36)));
		
		TimeSeries rel = ts.relativeSeriesIgnoreDescending();
		assertEquals( 4, rel.size() );
		assertEquals( new Double(12), rel.get(new Long(20)));
		assertEquals( new Double(8), rel.get(new Long(30)));
		assertEquals( new Double(0), rel.get(new Long(40)));
		assertEquals( new Double(6), rel.get(new Long(50)));
	}
}



