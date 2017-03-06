package qa.qcri.rtsm.process;

import java.util.Date;

import qa.qcri.rtsm.item.URLSeenFacebook;
import qa.qcri.rtsm.persist.TimeSeriesPersister;
import qa.qcri.rtsm.util.IntervalCounter;
import qa.qcri.rtsm.util.IntervalCounter.AlreadyFlushedException;
import qa.qcri.rtsm.util.Util;

public class TimeSeriesFacebookPreparePersistPE extends PersistedAbstractPE {

	public final static String KEY_LIKES_ONE_MINUTE = "f_l_" + IntervalCounter.STR_ONE_MINUTE;

	public final static String KEY_LIKES_ONE_HOUR = "f_l_" + IntervalCounter.STR_ONE_HOUR;
	
	public final static String KEY_SHARES_ONE_MINUTE = "f_s_" + IntervalCounter.STR_ONE_MINUTE;

	public final static String KEY_SHARES_ONE_HOUR = "f_s_" + IntervalCounter.STR_ONE_HOUR;

	IntervalCounter likesPerMinute;
	
	IntervalCounter likesPerHour;
	
	IntervalCounter sharesPerMinute;

	IntervalCounter sharesPerHour;

	public int lastLikes = -1;
	
	public int lastShares = -1;
	
	private TimeSeriesPersister timeSeriesPersister;

	public void processEvent(URLSeenFacebook urlSeenFacebook) {
		int likes = (int) urlSeenFacebook.getLikes();
		int shares = (int) urlSeenFacebook.getShares();
		
		Util.logDebug(this, "Read facebook likes/shares for " + urlSeenFacebook.getUrl() + ": " + likes + "/" + shares );
		
		// Only write if the value has changed since the last time it was queried
		if ( likes != lastLikes || shares != lastShares ) {
			long now = (new Date()).getTime();

			if( likes != lastLikes ) {
				Util.logTrace(this, "Updating likes for " + urlSeenFacebook.getUrl() + ": " + likes);
				process( now, likes, likesPerMinute );
				process( now, likes, likesPerHour );
				lastLikes = likes;
			}
			
			if( shares != lastShares ) {
				Util.logTrace(this, "Updating shares for " + urlSeenFacebook.getUrl() + ": " + shares);
				process( now, shares, sharesPerMinute );
				process( now, shares, sharesPerHour );
				lastShares = shares;
			}
		}
	}
	
	private void process(long now, int count, IntervalCounter counter) {
		try {
			counter.setCounter(now, count);
		} catch (AlreadyFlushedException e) {
			e.printStackTrace();
			Util.logWarning(this, "Inconsistent: time period was already flushed");
		}
	}

	@Override
	public void output() {
		String url = (String) this.getKeyValue().get(0);
		long now = (new Date()).getTime();
		
		Util.logTrace(this, "Persisting per-minute facebook likes for " + url + ": " + likesPerMinute.toString() );
		timeSeriesPersister.set(KEY_LIKES_ONE_MINUTE, url, likesPerMinute.flush(now));
		
		Util.logTrace(this, "Persisting per-hour facebook likes for " + url + ": " + likesPerHour.toString() );
		timeSeriesPersister.set(KEY_LIKES_ONE_HOUR, url, likesPerHour.flush(now));
		
		Util.logTrace(this, "Persisting per-minute facebook shares for " + url + ": " + sharesPerMinute.toString() );
		timeSeriesPersister.set(KEY_SHARES_ONE_MINUTE, url, sharesPerMinute.flush(now));
		
		Util.logTrace(this, "Persisting per-hour facebook shares for " + url + ": " + sharesPerHour.toString() );
		timeSeriesPersister.set(KEY_SHARES_ONE_HOUR, url, sharesPerHour.flush(now));
	}

	@Override
	public void initInstance() {
		super.initInstance();
		
		likesPerMinute = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		likesPerHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
		lastLikes = -1;
		
		sharesPerMinute = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		sharesPerHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
		lastShares = -1;

		timeSeriesPersister = (TimeSeriesPersister) persister;
	}
}
