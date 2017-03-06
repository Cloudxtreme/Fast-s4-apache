package qa.qcri.rtsm.util;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import qa.qcri.rtsm.item.URLSeen;
import qa.qcri.rtsm.item.URLSeenCounter;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * Keeps a heap of the URLs with the largest count, evicting those with the smaller count once the capacity is reached.
 * 
 * @author chato
 *
 */
public class TopURLSeenByCounter {
	private MinMaxPriorityQueue<URLSeenCounter> elements;
	
	private ConcurrentHashMap<String,URLSeenCounter> map;

	private int capacity;

	public TopURLSeenByCounter(int capacity) {
		this.map = new ConcurrentHashMap<String,URLSeenCounter>();
		this.capacity = capacity;
		
		this.elements = MinMaxPriorityQueue.orderedBy(new URLSeenCounter.ByAscendingOrderOfVisitsComparator()).create();
	}

	public synchronized boolean add(URLSeenCounter urlSeenCounter) {
		if( map.containsKey(urlSeenCounter.getUrl())) {
			URLSeenCounter existing = map.get(urlSeenCounter.getUrl());
			elements.remove(existing);
		}
		
		boolean changed = elements.add(urlSeenCounter);
		if (elements.size() > capacity) {
			String removedURL = elements.peekLast().getUrl();
			map.remove(removedURL);
			URLSeenCounter removed = elements.removeLast();
			if( removed.getUrl().equals(urlSeenCounter.getUrl())) {
				return false;
			}
		}
		
		map.put(urlSeenCounter.getUrl(), urlSeenCounter);
		
		if( map.size() != elements.size() ) {
			throw new IllegalStateException();
		}
		return changed;
	}

	public synchronized URLSeen peek() {
		return elements.peekFirst();
	}
	
	public synchronized int size() {
		return elements.size();
	}
	
	public synchronized boolean containsURL(String url) {
		return map.containsKey(url);
	}
	
	public synchronized ConcurrentHashMap<String,Integer> urls() {
		// Returns a clone of the URLs in this set
		ConcurrentHashMap<String,Integer> urls = new ConcurrentHashMap<String,Integer>(elements.size());
		for( Entry<String, URLSeenCounter> entry: map.entrySet() ) {
			urls.put( entry.getKey(), new Integer(entry.getValue().getCount()) );
		}
		return urls;
	}
}