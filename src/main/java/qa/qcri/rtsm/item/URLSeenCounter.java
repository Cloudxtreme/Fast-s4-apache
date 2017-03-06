package qa.qcri.rtsm.item;

import java.util.Comparator;

import org.json.JSONObject;

import com.google.common.collect.ImmutableMap;

public class URLSeenCounter extends URLSeen {

	protected int count;
	
	protected long monitoredSince;
	
	public static class ByAscendingOrderOfVisitsComparator implements Comparator<URLSeenCounter> {
		@Override
		public int compare(URLSeenCounter o1, URLSeenCounter o2) {
			if (o1.getCount() <= o2.getCount()) {
				return 1;
			} else if (o1.getCount() >= o2.getCount()) {
				return -1;
			} else {
				return 0;
			}
		}
	}
	
	public URLSeenCounter() {

	}
	
	public URLSeenCounter(String site, String url, int count, long monitoredSince) {
		super(site, url);
		this.count = count;
		this.monitoredSince = monitoredSince;
	}
	
	@Override
	public String toString() {
		return (new JSONObject(ImmutableMap.of( "site", site, "url", url, "count", new Integer(count), "monitoredSince", new Long(monitoredSince) ))).toString();
	}
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public long getMonitoredSince() {
		return monitoredSince;
	}

	public void setMonitoredSince(long monitoredSince) {
		this.monitoredSince = monitoredSince;
	}
}
