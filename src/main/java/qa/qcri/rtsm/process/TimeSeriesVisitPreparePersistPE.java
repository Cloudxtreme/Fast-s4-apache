package qa.qcri.rtsm.process;

import java.util.Date;

import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.persist.TimeSeriesPersister;
import qa.qcri.rtsm.util.IntervalCounter;
import qa.qcri.rtsm.util.IntervalCounter.AlreadyFlushedException;
import qa.qcri.rtsm.util.Util;


public class TimeSeriesVisitPreparePersistPE extends PersistedAbstractPE {
	
	public final static String KEY_ONE_MINUTE = "v_" + IntervalCounter.STR_ONE_MINUTE;
	public final static String KEY_ONE_HOUR = "v_" + IntervalCounter.STR_ONE_HOUR;

	private TimeSeriesPersister timeSeriesPersister;
	
	protected IntervalCounter perMinute;
	protected IntervalCounter perHour;
	protected int lastCummulativeCount;
	protected long monitoredSince;
	
    public void processEvent(URLSeenCounter urlSeenCounter) {
    	
    	if( urlSeenCounter.getMonitoredSince() != this.monitoredSince ) {

    		if( this.monitoredSince != -1 ) {
    			Util.logWarning(this, "The counter for '" + this.getKeyValue().get(0) + "' changed epoch, cleared" );
    		}
    		
    		// A new "epoch" started
    		// The parent has been destroyed and restarted, meaning that the lastCummulativeCount is not valid
    		this.setCountersToZero();
    		this.monitoredSince = urlSeenCounter.getMonitoredSince();
    	}
    	
    	int cummulativeCount = urlSeenCounter.getCount();
    	int segmentCount = cummulativeCount - lastCummulativeCount;
	//Util.logInfo(this, "url "+urlSeenCounter.getUrl()+"  |cummulativeCounti:"+cummulativeCount+"  |lastCummulativeCount:"+lastCummulativeCount+" |segmentCount:"+segmentCount);
    	long now = (new Date()).getTime();
    	try {
			perMinute.incrementCounter(now, segmentCount);
			perHour.incrementCounter(now, segmentCount);
		} catch (AlreadyFlushedException e) {
			e.printStackTrace();
			Util.logWarning(this, "Inconsistent: time period was already flushed" );
		}
    	lastCummulativeCount = cummulativeCount;
    }

    @Override
    public void output() {
		String url = (String) this.getKeyValue().get(0);
    	long now = (new Date()).getTime();
		timeSeriesPersister.set(KEY_ONE_MINUTE, url, perMinute.flush(now) );
		timeSeriesPersister.set(KEY_ONE_HOUR, url, perHour.flush(now) );
    }

	@Override
	public void initInstance() {
		super.initInstance();
		timeSeriesPersister = (TimeSeriesPersister)persister;
		perMinute = new IntervalCounter(IntervalCounter.ONE_MINUTE);
		perHour = new IntervalCounter(IntervalCounter.ONE_HOUR);
		monitoredSince = -1;
		setCountersToZero();
	}
	
	public void setCountersToZero() {
		perMinute.clear();
		perHour.clear();
		lastCummulativeCount = 0;
	}
}
