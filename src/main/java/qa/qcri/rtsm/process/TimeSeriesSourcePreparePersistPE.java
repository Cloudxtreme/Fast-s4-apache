package qa.qcri.rtsm.process;

import java.util.Date;

import qa.qcri.rtsm.item.URLSeenSource;
import qa.qcri.rtsm.item.URLSeenSource.SourceType;
import qa.qcri.rtsm.persist.TimeSeriesPersister;
import qa.qcri.rtsm.util.IntervalCounter;
import qa.qcri.rtsm.util.IntervalCounter.AlreadyFlushedException;
import qa.qcri.rtsm.util.Util;

public class TimeSeriesSourcePreparePersistPE extends PersistedAbstractPE {

	final public static String KEY_ORGANIC_ONE_MINUTE = "s_" + SourceType.ORGANIC.getSymbol() + "_" + IntervalCounter.STR_ONE_MINUTE;

	final public static String KEY_ORGANIC_ONE_HOUR = "s_" + SourceType.ORGANIC.getSymbol() + "_" + IntervalCounter.STR_ONE_HOUR;

	final public static String KEY_DIRECT_ONE_MINUTE = "s_" + SourceType.DIRECT.getSymbol() + "_" + IntervalCounter.STR_ONE_MINUTE;

	final public static String KEY_DIRECT_ONE_HOUR = "s_" + SourceType.DIRECT.getSymbol() + "_" + IntervalCounter.STR_ONE_HOUR;

	final public static String KEY_REFERRAL_ONE_MINUTE = "s_" + SourceType.REFERRAL.getSymbol() + "_" + IntervalCounter.STR_ONE_MINUTE;

	final public static String KEY_REFERRAL_ONE_HOUR = "s_" + SourceType.REFERRAL.getSymbol() + "_" + IntervalCounter.STR_ONE_HOUR;
	
	final public static String KEY_INTERNAL_ONE_MINUTE = "s_" + SourceType.INTERNAL.getSymbol() + "_" + IntervalCounter.STR_ONE_MINUTE;

	final public static String KEY_INTERNAL_ONE_HOUR = "s_" + SourceType.INTERNAL.getSymbol() + "_" + IntervalCounter.STR_ONE_HOUR;

	private TimeSeriesPersister timeSeriesPersister;

	private IntervalCounter organicOneMinute;

	private IntervalCounter organicOneHour;

	private IntervalCounter directOneMinute;

	private IntervalCounter directOneHour;

	private IntervalCounter referralOneMinute;

	private IntervalCounter referralOneHour;
	
	private IntervalCounter internalOneMinute;

	private IntervalCounter internalOneHour;

	public void processEvent(URLSeenSource urlSeenSource) {
		long now = (new Date()).getTime();

		try {
			switch (urlSeenSource.sourceType()) {
			case ORGANIC:
				organicOneMinute.incrementCounter(now, 1);
				organicOneHour.incrementCounter(now, 1);
				break;
			case DIRECT:
				directOneMinute.incrementCounter(now, 1);
				directOneHour.incrementCounter(now, 1);
				break;
			case REFERRAL:
				referralOneMinute.incrementCounter(now, 1);
				referralOneHour.incrementCounter(now, 1);
				break;
			case INTERNAL:
				internalOneMinute.incrementCounter(now, 1);
				internalOneHour.incrementCounter(now, 1);
				break;
			default:
				throw new IllegalArgumentException("Unknown source type");
			}
		} catch (AlreadyFlushedException e) {
			e.printStackTrace();
			Util.logWarning(this, "Inconsistent: time period was already flushed");
		}
	}

	@Override
	public void output() {
		String url = (String) this.getKeyValue().get(0);
		long now = (new Date()).getTime();
		timeSeriesPersister.set(KEY_ORGANIC_ONE_MINUTE, url, organicOneMinute.flush(now));
		timeSeriesPersister.set(KEY_ORGANIC_ONE_HOUR, url, organicOneHour.flush(now));
		timeSeriesPersister.set(KEY_DIRECT_ONE_MINUTE, url, directOneMinute.flush(now));
		timeSeriesPersister.set(KEY_DIRECT_ONE_HOUR, url, directOneHour.flush(now));
		timeSeriesPersister.set(KEY_REFERRAL_ONE_MINUTE, url, referralOneMinute.flush(now));
		timeSeriesPersister.set(KEY_REFERRAL_ONE_HOUR, url, referralOneHour.flush(now));
		timeSeriesPersister.set(KEY_INTERNAL_ONE_MINUTE, url, internalOneMinute.flush(now));
		timeSeriesPersister.set(KEY_INTERNAL_ONE_HOUR, url, internalOneHour.flush(now));
	}

	@Override
	public void initInstance() {
		super.initInstance();
		timeSeriesPersister = (TimeSeriesPersister) persister;
		
		organicOneMinute = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		directOneMinute  = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		referralOneMinute = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		internalOneMinute = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		
		organicOneHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
		directOneHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
		referralOneHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
		internalOneHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
	}
}