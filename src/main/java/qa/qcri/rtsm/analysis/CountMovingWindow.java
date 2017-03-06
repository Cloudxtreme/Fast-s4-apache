package qa.qcri.rtsm.analysis;

import qa.qcri.rtsm.util.IntervalCounter;
import qa.qcri.rtsm.util.IntervalCounter.AlreadyFlushedException;

public class CountMovingWindow {
	
	final static long MIN_INTERVAL = IntervalCounter.ONE_MINUTE;
	
	private long windowSize;
	
	private IntervalCounter intervalCounter;
	
	private long lastQueryTime;

	public CountMovingWindow(long windowSize) {
		if( windowSize < MIN_INTERVAL ) {
			throw new IllegalArgumentException("Minimum window size is " + windowSize );
		}
		this.windowSize = windowSize;
		this.intervalCounter = new IntervalCounter(MIN_INTERVAL);
		this.lastQueryTime = Long.MIN_VALUE;
	}
	
	public void insertEvent(long eventTime) {
		insertEvent(eventTime, 1);
	}
	
	public void insertEvent(long eventTime, int count) {
		if( eventTime < lastQueryTime ) {
			throw new IllegalArgumentException("Event is before the last query time");
		}
		try {
			intervalCounter.incrementCounter(eventTime, count);
		} catch (AlreadyFlushedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Already flushed");
		}
	}
	
	public int getCount(long now) {
		if( now < lastQueryTime ) {
			throw new IllegalArgumentException("Now is before the last query time");
		}
		intervalCounter.flush( now - windowSize );
		lastQueryTime = now;
		return intervalCounter.getSumValues();
	}
}
