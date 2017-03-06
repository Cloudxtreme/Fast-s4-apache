package qa.qcri.rtsm.process;

import qa.qcri.rtsm.item.SiteConfigurations;
import qa.qcri.rtsm.item.URLSeenSample;
import qa.qcri.rtsm.item.URLSeenSource;
import qa.qcri.rtsm.item.Visit;
import qa.qcri.rtsm.util.Util;
import java.net.URLDecoder;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;


public class URLExtractorPE extends DispatchedOutputedAbstractPE {

	protected String outputStreamNameSource;
	
	protected SiteConfigurations sites;
	
    public void processEvent(Visit visit) { 
    	if( sites == null ) {
    		Util.logError(this, "List of enabled sites not set");
    		return;
    	} else if( ! sites.hasSiteId(visit.getSiteID()) ) {
    		Util.logWarning(this, "Ignoring visit having siteID '" + visit.getSiteID() + "', which is not registered in the configuration");
    		return;
    	} else if( ! sites.hasURLPattern(visit.getUrl(),visit.getSiteID())) {
		Util.logWarning(this, "Ignoring visit not having .html in URL for siteID... '" + visit.getUrl() + "'");
    		return;
    	}
    	
	//Util.logWarning(this, "Initial Url " +visit.getUrl());
	//Util.logWarning(this, "Initial Url normalized " +visit.getStrippedNormalizedUrl());
        URLSeenSample urlSeenSample = null;
	URLSeenSource urlSeenSource= null;
	if(visit.getUrl().contains("www.aljazeera.net")){
		String url = visit.getUrl();
		url = url.replaceFirst("https?://", "");
		String[] tokensRaw = url.split("/");
		ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
		for( int i=0; i<tokensRaw.length; i++ ) {
			try {
				tokensEncoded.add(i, URLDecoder.decode(tokensRaw[i], Util.UTF8.toString()));
			} catch (UnsupportedEncodingException e) {
				tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());	
			}
		}
		url="http://"+StringUtils.join(tokensEncoded, "/");
                //Util.logDebug(this, "url decoded: " + url);

        	urlSeenSample = new URLSeenSample(visit.getSiteID(), url, visit.getSampleRate());
        	urlSeenSource = new URLSeenSource(visit.getSiteID(), url, visit);
	}else{
		String url = visit.getStrippedNormalizedUrl();
		 if(url.contains("dohanews.co") && !url.endsWith("/"))
	                   url= url+"/";
        	Util.logDebug(this, "url: " + url);
        	urlSeenSample = new URLSeenSample(visit.getSiteID(), url, visit.getSampleRate());
        	urlSeenSource = new URLSeenSource(visit.getSiteID(), url, visit);
	}

        dispatcher.dispatchEvent(outputStreamName, urlSeenSample); 
        dispatcher.dispatchEvent(outputStreamNameSource, urlSeenSource);
    }

    @Override
    public void output() {
        // No-op
    	// We generate events as soon as they arrive
    }

	public String getOutputStreamNameSource() {
		return outputStreamNameSource;
	}

	public void setOutputStreamNameSource(String outputStreamNameSource) {
		this.outputStreamNameSource = outputStreamNameSource;
	}

	public SiteConfigurations getSites() {
		return sites;
	}

	public void setSites(SiteConfigurations sites) {
		this.sites = sites;
		Util.logInfo(this, "Configured sites: " + sites.listIds());
	}
}
