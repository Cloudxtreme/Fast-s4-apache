package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.analysis.CountMovingWindow;

public class CountMovingWindowTest {

	DateFormat d;
	
	@Before
	public void setUp() throws Exception {
		d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	@Test
	public void testCountMovingWindow() {
		CountMovingWindow cmw = new CountMovingWindow(IntervalCounter.ONE_HOUR);
		assertTrue( cmw != null );
		try {
			new CountMovingWindow(IntervalCounter.ONE_SECOND);
			fail("Should have thrown an illegal argument exception");
		} catch( IllegalArgumentException e ) {
			assertTrue(true);
		}
	}

	@Test
	public void testInsertEventGetCount1() throws ParseException {
		CountMovingWindow cmw = new CountMovingWindow(IntervalCounter.ONE_HOUR);
		cmw.insertEvent( d.parse("2000-01-01 09:00:01").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:01:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:03:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:05:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:32:05").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:58:05").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 10:30:00").getTime() );
		
		assertEquals( 3, cmw.getCount( d.parse("2000-01-01 10:31:00").getTime() ) ); 
	}
	
	@Test
	public void testInsertEventGetCount2() throws ParseException {
		CountMovingWindow cmw = new CountMovingWindow(IntervalCounter.ONE_HOUR);
		cmw.insertEvent( d.parse("2000-01-01 09:00:01").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:01:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:03:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:05:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:32:05").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:58:05").getTime() );
		
		assertEquals( 6, cmw.getCount( d.parse("2000-01-01 10:00:00").getTime() ) ); 
	}
	
	@Test
	public void testInsertEventGetCount3() throws ParseException {
		CountMovingWindow cmw = new CountMovingWindow(IntervalCounter.ONE_HOUR);
		cmw.insertEvent( d.parse("2000-01-01 09:00:01").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:01:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:03:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:05:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:32:05").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 09:58:05").getTime() );
		
		assertEquals( 6, cmw.getCount( d.parse("2000-01-01 10:00:00").getTime() ) ); 
		
		cmw.insertEvent( d.parse("2000-01-01 10:30:00").getTime() );
		
		assertEquals( 3, cmw.getCount( d.parse("2000-01-01 10:31:00").getTime() ) );
		
		cmw.insertEvent( d.parse("2000-01-01 10:32:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 10:33:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 10:34:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 10:35:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 10:38:00").getTime() );
		cmw.insertEvent( d.parse("2000-01-01 10:59:59").getTime() );
		
		for( int i=0; i<10; i++ ) {
			assertEquals( 7, cmw.getCount( d.parse("2000-01-01 11:00:00").getTime() ) );
		}
		
		for( int i=0; i<60; i++ ) {
			cmw.insertEvent( d.parse("2000-01-01 11:01:" + String.format("%2d", new Integer(i)) ).getTime() );
		}
		
		for( int i=0; i<10; i++ ) {
			assertEquals( 67, cmw.getCount( d.parse("2000-01-01 11:02:00").getTime() ) );
		}
		
		try {
			cmw.getCount( d.parse("2000-01-01 11:01:00").getTime() );
			fail("Should have thrown an exception");
		} catch( IllegalArgumentException e ) {
			assertTrue(true);
		}
		
		try {
			cmw.insertEvent( d.parse("2000-01-01 11:01:00").getTime() );
			fail("Should have thrown an exception");
		} catch( IllegalArgumentException e ) {
			assertTrue(true);
		}
	}
}

