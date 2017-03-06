package qa.qcri.rtsm.item;

import org.json.JSONObject;

import com.google.common.collect.ImmutableMap;

public class URLSeenSample extends URLSeen {

	protected double sampleRate;

	protected String url;
	
	public URLSeenSample() {

	}
	
	public URLSeenSample(String site, String url, double sampleRate) {
		super(site, url);
		this.setSampleRate(sampleRate);
		this.url=url;
	}
	
	@Override
	public String toString() {
		return (new JSONObject(ImmutableMap.of( "site", site, "url", url, "sampleRate", new Float(sampleRate) ))).toString();
	}

	public double getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public String getUrl(){
		return this.url;
	}
}
