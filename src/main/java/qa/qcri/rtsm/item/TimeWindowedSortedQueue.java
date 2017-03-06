package qa.qcri.rtsm.item;

import java.util.Date;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.Semaphore;


public class TimeWindowedSortedQueue<T> {
	
	public static class Element<T> implements Comparable<Element<T>> {
		final long timestamp;
		final T item;
		
		public Element(long timestamp, T item) {
			this.timestamp = timestamp;
			this.item = item;
		}
		
		@Override
		public int compareTo(Element<T> o) {
			if(timestamp < o.getTimestamp() ) {
				return -1;
			} else if( timestamp > o.getTimestamp() ) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public boolean equals(Object obj) {
			if( obj instanceof Element<?>) {
				return this.item.equals(((Element<?>) obj).getItem());
			} else {
				return false;
			}
		}		
		
		public long getTimestamp() {
			return timestamp;
		}
		
		public T getItem() {
			return item;
		}
	}
	
	protected final TreeSet<Element<T>> list;
	
	protected final int windowSizeMillis;
	
	protected final Semaphore semaphore;
	
	public TimeWindowedSortedQueue(int windowSizeMillis) {
		this.list = new TreeSet<Element<T>>();
		this.windowSizeMillis = windowSizeMillis;
		this.semaphore = new Semaphore(1);
	}
	
	public void insertElement(T item) {
		long now = (new Date()).getTime();
		insertElement(now,item);
	}
	
	public void insertElement(long now, T item) {
		Element<T> element = new Element<T>(now, item);
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		list.add(element);
		semaphore.release();
	}
	
	public Vector<T> flushOldElements() {
		long now = (new Date()).getTime();
		return flushOldElements(now);
	}
	
	public Vector<T> flushOldElements(long now) {
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		Iterator<Element<T>> it = list.iterator();
		Vector<Element<T>> resultElements = new Vector<Element<T>>();
		while( it.hasNext() ) {
			Element<T> item = it.next();
			if( item.getTimestamp() <= now - windowSizeMillis ) {
				resultElements.add(item);
			}
		}
		Vector<T> result = new Vector<T>(resultElements.size());
		long lastTimestamp = Long.MIN_VALUE; 
		for( Element<T> element: resultElements ) {
			
			// Check timestamps
			if( element.getTimestamp() < lastTimestamp ) {
				throw new IllegalStateException("Elements were not sorted by timestamp");
			}
			lastTimestamp = element.getTimestamp();
			
			// Add item
			T item = element.getItem();
			result.add(item);
			list.remove(element);
		}
		semaphore.release();
		return result;
	}

	public int size() {
		return list.size();
	}
}