package qa.qcri.rtsm.analysis;

import static org.junit.Assert.assertEquals;

import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

//

public class MovingAverageTest {

	TreeMap<Long, Integer> input;
	
	@Before
	public void setUp() throws Exception {
		input = new TreeMap<Long, Integer>();
		input.put( new Long(20), new Integer(4) );
		input.put( new Long(30), new Integer(9) );
		input.put( new Long(40), new Integer(5) );
		input.put( new Long(60), new Integer(8) );
		input.put( new Long(80), new Integer(12) );
		input.put( new Long(89), new Integer(7) );
	}
	
	@Test
	public void testComputeByDurationForKey() {
		MovingAverage<Integer> ma1 = new MovingAverage<Integer>(30, 10);
		assertEquals(4, ma1.computeByDurationForKey(input, 20), 1e-8 );
		assertEquals((5+0+8)/3.0, ma1.computeByDurationForKey(input, 60), 1e-8);

		MovingAverage<Integer> ma2 = new MovingAverage<Integer>(10,1);
		assertEquals(4, ma2.computeByDurationForKey(input, 20), 1e-8 );
		assertEquals((5+0+0+0+0+0+0+0+0+0)/10.0, ma2.computeByDurationForKey(input, 40), 1e-8 );
		assertEquals((7+0+0+0+0+0+0+0+0+12)/10.0, ma2.computeByDurationForKey(input, 89), 1e-8 );
		
		MovingAverage<Integer> ma3 = new MovingAverage<Integer>(30,15);
		assertEquals(4, ma3.computeByDurationForKey(input, 20), 1e-8 );
		assertEquals((4+9+5)/3.0, ma3.computeByDurationForKey(input, 40), 1e-8 );
		assertEquals((8+12+7)/3.0, ma3.computeByDurationForKey(input, 89), 1e-8 );
		
	}

	@Test
	public void testComputeByDuration() {
		MovingAverage<Integer> ma = new MovingAverage<Integer>(29, 10);
		
		SortedMap<Long, Double> output = ma.computeByDuration(input);
		assertEquals( input.size(), output.size() );
		
		assertEquals( ((4)/1.0), output.get(new Long(20)).doubleValue(), 1e-8 );
		assertEquals( ((4+9)/2.0), output.get(new Long(30)).doubleValue(), 1e-8 );
		assertEquals( ((4+9+5)/3.0), output.get(new Long(40)).doubleValue(), 1e-8 );
		assertEquals( ((5+0+8)/3.0), output.get(new Long(60)).doubleValue(), 1e-8 );
		assertEquals( ((8+12+0)/3.0), output.get(new Long(80)).doubleValue(), 1e-8 );
		assertEquals( ((0+12+7)/3.0), output.get(new Long(89)).doubleValue(), 1e-8 );
	}
}
