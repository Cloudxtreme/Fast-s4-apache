package qa.qcri.rtsm.util;

import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class IntervalCounter {

	public static long ONE_SECOND = 1000;
	public static String STR_ONE_SECOND = "1s";

	public static long TEN_SECONDS = 10 * ONE_SECOND;
	public static String STR_TEN_SECONDS = "10s";

	public static long ONE_MINUTE = 60 * ONE_SECOND;
	public static String STR_ONE_MINUTE = "1m";

	public static long FIVE_MINUTES = 5 * ONE_MINUTE;
	public static String STR_FIVE_MINUTES = "5m";

	public static long FIFTEEN_MINUTES = 15 * ONE_MINUTE;
	public static String STR_FIFTEEN_MINUTES = "15m";

	public static long THIRTY_MINUTES = 30 * ONE_MINUTE;
	public static String STR_THIRTY_MINUTES = "30m";

	public static long ONE_HOUR = 60 * ONE_MINUTE;
	public static String STR_ONE_HOUR = "1h";

	long intervalSize;

	TreeMap<Long,Integer> counter;
	long lastTimeFlushed;
	final Semaphore insertSemaphore;
	
	public class AlreadyFlushedException extends Exception {
		public AlreadyFlushedException(String string) {
			super(string);
		}

		private static final long serialVersionUID = 1L;
	}

	public IntervalCounter(long intervalSize) {
		this.intervalSize = intervalSize;
		this.counter = new TreeMap<Long, Integer>();
		this.lastTimeFlushed = Long.MIN_VALUE;
		this.insertSemaphore = new Semaphore(1);
	}
	
	public void clear() {
		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		this.counter.clear();
		this.lastTimeFlushed = Long.MIN_VALUE;
		insertSemaphore.release();
	}

	public void incrementCounter(long eventTime) throws AlreadyFlushedException {
		incrementCounter(eventTime, 1);
	}

	public void incrementCounter(long eventTime, int count) throws AlreadyFlushedException {
		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		if (eventTime < lastTimeFlushed) {
			insertSemaphore.release();
			throw new AlreadyFlushedException("Event time is " + eventTime + " but this was last flushed at " + lastTimeFlushed);
		}
		Long startOfInterval = new Long(startOfInterval(eventTime));
		if (counter.containsKey(startOfInterval)) {
			int previousValue = counter.get(startOfInterval).intValue();
			counter.put(startOfInterval, new Integer(previousValue + count));
		} else {
			counter.put(startOfInterval, new Integer(count));
		}
		insertSemaphore.release();
	}
	
	public void setCounter(long eventTime, int count) throws AlreadyFlushedException {
		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		if (eventTime < lastTimeFlushed) {
			insertSemaphore.release();
			throw new AlreadyFlushedException("Event time is " + eventTime + " but this was last flushed at " + lastTimeFlushed);
		}
		Long startOfInterval = new Long(startOfInterval(eventTime));
		counter.put(startOfInterval, new Integer(count));
		insertSemaphore.release();
	}
	
	public int getCounter(long startOfInterval) throws AlreadyFlushedException {
		if( startOfInterval % intervalSize != 0 ) {
			throw new IllegalArgumentException( "The start of interval " + startOfInterval + " is not valid for intervals of size " + intervalSize );
		}
		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		if (startOfInterval < lastTimeFlushed) {
			throw new AlreadyFlushedException("Start of interval is " + startOfInterval + " but this was last flushed at " + lastTimeFlushed);
		}
		Integer cntInteger = counter.get(new Long(startOfInterval)); 
		int cntInt = ( cntInteger != null ) ? cntInteger.intValue() : 0;
		insertSemaphore.release();
		return cntInt;
	}

	long startOfInterval(long eventTime) {
		return (long) Math.floor(eventTime / intervalSize) * (long) intervalSize;
	}

	public TreeMap<Long, Integer> flush(long currentTime) {
		long oldestPreservedStartOfInterval = startOfInterval(currentTime);
		TreeMap<Long, Integer> oldCounter = new TreeMap<Long, Integer>();

		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		
		// Copy values
		FOR: for (Long startOfInterval : counter.keySet()) {
			if (startOfInterval.longValue() < oldestPreservedStartOfInterval) {
				oldCounter.put(startOfInterval, counter.get(startOfInterval));
			} else {
				// This is larger, and the ones that follow are also larger
				// because the TreeSet returns elements in ascending order
				break FOR;
			}
		}

		// Remove copied values from counters
		if( oldCounter.size() > 0 ) {
			for (Long startOfInterval : oldCounter.keySet()) {
				counter.remove(startOfInterval);
			}
		}

		lastTimeFlushed = oldestPreservedStartOfInterval;
		
		insertSemaphore.release();
		return oldCounter;
	}
	
	public String toString() {
		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		
		StringBuffer sb = new StringBuffer();
		for( Long startOfInterval : counter.keySet() ) {
			sb.append( "(" + startOfInterval + ", " + counter.get(startOfInterval) + ") ");
		}
		
		insertSemaphore.release();
		return sb.toString();
	}
	
	public int getNumIntervals() {
		// No need to acquire semaphore because counter is thread-safe
		return counter.size();
	}
	
	public int getSumValues() {
		try {
			insertSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		int sum = 0;
		for( Integer val: counter.values() ) {
			sum += val.intValue();
		}
		insertSemaphore.release();
		return sum;
	}
}
