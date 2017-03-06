package qa.qcri.rtsm.process;

import java.util.Date;

import qa.qcri.rtsm.analysis.CountMovingWindow;
import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.item.URLSeenSample;
import qa.qcri.rtsm.util.IntervalCounter;
import qa.qcri.rtsm.util.Util;

public class URLVisitsCountPE extends DispatchedOutputedAbstractPE {
	
	private String site;
	
	private int threshold;

	private double totalCount;
	
	private long monitoredSince;

	private CountMovingWindow lastHourCount;
	
	private CountMovingWindow lastFiveMinutesCount;

	private CountMovingWindow lastFifteenMinutesCount;

	private String outputStreamNameOneHour;
	
	private String outputStreamNameFiveMinutes;
	
	public int getThreshold() {
		return threshold;
	}

	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}

	public void processEvent(URLSeenSample urlSeenSample) {
		if( site == null ) {
			site = urlSeenSample.getSite();
		} else if( ! site.equals(urlSeenSample.getSite())) {
			Util.logError( this, "There was one PE per site but site changed, was: '" + site + "', now it is '" + urlSeenSample.getSite() + "'");
			return;
		}
		
		// Determine how many visits to add
		double sampleRate = urlSeenSample.getSampleRate();
		double visitsToAdd;
		if (Math.signum(sampleRate) > 0.0) {
			visitsToAdd = (1.0 / (double) sampleRate);
		} else {
			visitsToAdd = 1.0;
		}

		// Add this number of visits to the total expected count
		totalCount += visitsToAdd;
               // Util.logDebug(this,"sampleiRate:"+sampleRate+"  visitsToAdd:"+visitsToAdd+" totalCount:"+totalCount);
		long now = (new Date()).getTime();
		
	//	Util.logDebug(this, "processEvent--->totalCount " + totalCount +" visitsToAdd:"+visitsToAdd +" url:"+urlSeenSample.getUrl()+ "   site:"+site);
		// Add this number of visits to the hourly count
		//Util.logDebug(this,"urll:"+urlSeenSample.getUrl()+"|Count:"+visitsToAdd+"|"+(int) Math.round(visitsToAdd));
		lastHourCount.insertEvent(now, (int) Math.round(visitsToAdd));
		
		// Add this number of visits to the five-minutes count
		lastFiveMinutesCount.insertEvent(now, (int)Math.round(visitsToAdd));
	}

	@Override
	public void initInstance() {
		site = null;
		lastHourCount = new CountMovingWindow(IntervalCounter.ONE_HOUR);
		lastFiveMinutesCount = new CountMovingWindow(IntervalCounter.FIVE_MINUTES);
		lastFifteenMinutesCount = new CountMovingWindow(IntervalCounter.FIFTEEN_MINUTES);
		monitoredSince = (new Date()).getTime();
	}

	@Override
	public void output() {
		int count = (int) Math.round(totalCount);
		if( count < threshold) {
			//Util.logDebug(this, "Count " + count + " is less than threshold " + threshold + ", no output");
			return;
		}
		
		String url = (String) this.getKeyValue().get(0);
		URLSeenCounter urlSeenCounter = new URLSeenCounter(site, url, count, monitoredSince);
		dispatcher.dispatchEvent(outputStreamName, urlSeenCounter);
		Util.logDebug(this, "Dispatched " + urlSeenCounter + " to " + outputStreamName);
		
		long now = (new Date()).getTime();
		
               // Util.logDebug(this,"FAST  site:"+site+"  url:"+url);
		int countOneHour = lastHourCount.getCount(now);
		URLSeenCounter urlSeenCountOneHour = new URLSeenCounter(site, url, countOneHour, monitoredSince);
		dispatcher.dispatchEvent(outputStreamNameOneHour, urlSeenCountOneHour);
		Util.logDebug(this, "Dispatched " + urlSeenCountOneHour + " to " + outputStreamNameOneHour);
		
		int countFiveMinutes = lastFiveMinutesCount.getCount(now);
		URLSeenCounter urlSeenCountFiveMinutes = new URLSeenCounter(site, url, countFiveMinutes, monitoredSince);
		dispatcher.dispatchEvent(outputStreamNameFiveMinutes, urlSeenCountFiveMinutes);
		Util.logDebug(this, "Dispatched " + urlSeenCountFiveMinutes + " to " + outputStreamNameFiveMinutes);
//		Util.logDebug(this, "Last URLVisitsCount PE invoked 2 transferred data to TopNURLs");
	}

	public String getOutputStreamNameOneHour() {
		return outputStreamNameOneHour;
	}

	public void setOutputStreamNameOneHour(String outputStreamNameOneHour) {
		this.outputStreamNameOneHour = outputStreamNameOneHour;
	}

	public String getOutputStreamNameFiveMinutes() {
		return outputStreamNameFiveMinutes;
	}

	public void setOutputStreamNameFiveMinutes(String outputStreamNameFiveMinutes) {
		this.outputStreamNameFiveMinutes = outputStreamNameFiveMinutes;
	}
}
