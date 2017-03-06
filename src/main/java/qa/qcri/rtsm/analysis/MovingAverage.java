package qa.qcri.rtsm.analysis;

import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

public class MovingAverage<V extends Number> {
	int windowDuration;
	int granularity;

	public MovingAverage(int windowDuration, int granularity) {
		if( windowDuration <= granularity ) {
			throw new IllegalArgumentException("The granularity has to be smaller than the windowDuration");
		}
		this.windowDuration = windowDuration;
		this.granularity = granularity;
	}
	
	public SortedMap<Long, Double> computeByDuration(TreeMap<Long, V> data) {
		if( data == null ) {
			throw new IllegalArgumentException("Null input given");
		}
		
		TreeMap<Long,Double> output = new TreeMap<Long,Double>();
		for( Long key: data.keySet() ) {
			double value = computeByDurationForKey(data, key.longValue());
			output.put(key, new Double(value));
		}
		return output;
	}
	
	protected double computeByDurationForKey(TreeMap<Long, V> data, long key) {
		double sum = 0.0;
		int count = 0;
		long partInf = key - windowDuration;
		long partMax = partInf + granularity;
		
		if( partInf < data.firstKey().longValue() ) {
			partInf = data.firstKey().longValue() - granularity;
			partMax = data.firstKey().longValue();
		}
		
		while( partInf < key ) {
			NavigableMap<Long, V> subMap = data.subMap(new Long(partInf), false, new Long(partMax), true);	
			 
			if( subMap.size() == 0 ) {
				count++;
				sum += 0.0;
			} else {
				for( V value: subMap.values() ) {
					count++;
					sum += value.doubleValue();
				}
			}
			partInf += granularity;
			partMax += granularity;
		}
		return sum / (double)count;
	}
}
