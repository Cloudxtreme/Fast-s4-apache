package qa.qcri.rtsm.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import qa.qcri.rtsm.item.URLSeenCounter;

public class TopURLSeenByCounterWithLeastRecentlyUsed<V extends LastWorkedTime> {
	
	public static abstract class WorkerFactory<VV> {
		public abstract VV newInstance(String url);
	}

	private ConcurrentHashMap<String, V> workerPerURL;

	private TopURLSeenByCounter topURLsByCounter;
	
	private WorkerFactory<V> workerFactory;

	public TopURLSeenByCounterWithLeastRecentlyUsed(int urlsToMonitor, WorkerFactory<V> workerFactory) {
		this.workerPerURL = new ConcurrentHashMap<String, V>(urlsToMonitor);
		this.topURLsByCounter = new TopURLSeenByCounter(urlsToMonitor);
		this.workerFactory = workerFactory;
	}

	public void add(URLSeenCounter urlSeenCounter) {
		topURLsByCounter.add(urlSeenCounter);
	}
	
	public int size() {
		return topURLsByCounter.size();
	}
	
	public boolean containsURL(String string) {
		return topURLsByCounter.containsURL(string);
	}
	
	/**
	 * Synchronize searcherPerURL so it contains the same elements as monitored URLs
	 * 
	 * @param monitoredURLs list of monitored URLs
	 */
	private void synchronizeWorkerPerURL(ConcurrentHashMap<String, Integer> monitoredURLs) {
		for (String url : workerPerURL.keySet()) {
			if (!monitoredURLs.containsKey(url)) {
				workerPerURL.remove(url);
			}
		}
		for (String url : monitoredURLs.keySet()) {
			if (!workerPerURL.containsKey(url)) {
				workerPerURL.put(url, workerFactory.newInstance(url));
			}
		}
		if( workerPerURL.size() != monitoredURLs.size() ) {
			throw new IllegalStateException();
		}
	}
	
	private V findInactiveWorker(Vector<String> urls) {
		for (String url: urls) {
			V searcher = workerPerURL.get(url);
			if (! searcher.hasWorked()) {
				return searcher;
			}
		}
		return null;
	}
	
	private V findLeastRecentlyUsedWorker(Vector<String> urls) {
		V chosen = null;
		long oldestCheckedTime = Long.MAX_VALUE;
		for (String url: urls) {
			V searcher = workerPerURL.get(url);
			long checkedTime = searcher.getLastWorkedTime();
			if (checkedTime < oldestCheckedTime) {
				chosen = searcher;
				oldestCheckedTime = checkedTime;
			}
		}
		return chosen;
	}

	public V getNext() {
		// Copy because it is being constantly modified
		final ConcurrentHashMap<String, Integer> urlCounts = topURLsByCounter.urls();
		if (urlCounts.size() == 0) {
			Util.logDebug(this, "No URLs being monitored");
			return null;
		}
		synchronizeWorkerPerURL(urlCounts);

		// Sort by descending count
		Vector<String> urls = new Vector<String>(urlCounts.keySet());
		Collections.sort(urls, new Comparator<String>() {

			@Override
			public int compare(String url1, String url2) {
				int count1 = urlCounts.get(url1).intValue();
				int count2 = urlCounts.get(url2).intValue();
				if (count1 < count2) {
					return 1;
				} else if (count1 > count2) {
					return -1;
				} else {
					return 0;
				}
			}
		});

		V chosen = findInactiveWorker(urls);

		if (chosen == null) {
			chosen = findLeastRecentlyUsedWorker(urls);
		}
		
		return chosen;
	}
}
