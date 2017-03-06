/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package qa.qcri.rtsm.process;

import io.s4.dispatcher.EventDispatcher;
import qa.qcri.rtsm.facebook.FacebookQuerier;
import qa.qcri.rtsm.facebook.FacebookQuerierForURL;
import qa.qcri.rtsm.facebook.FacebookURLInfo;
import qa.qcri.rtsm.item.SiteConfigurations;
import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.item.URLSeenFacebook;
import qa.qcri.rtsm.util.TopURLSeenByCounterWithLeastRecentlyUsed;
import qa.qcri.rtsm.util.TopURLSeenByCounterWithLeastRecentlyUsed.WorkerFactory;
import qa.qcri.rtsm.util.Util;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * This PE keeps a set of "active" URLs and perform periodic Facebook queries for them.
 * 
 * @author chato
 * 
 */
public class FacebookQuerierPE extends LoggableAbstractPE {

	private String siteId;
	
	private EventDispatcher dispatcher;

	private String outputStreamName;

	private int urlsToMonitor;
	
	private SiteConfigurations sites;
	
	private FacebookQuerier fb;

	private TopURLSeenByCounterWithLeastRecentlyUsed<FacebookQuerierForURL> monitor;

	@Override
	public void initInstance() {
		super.initInstance();
		siteId = null;
		monitor = null;
	}
	
	/**
	 * @param siteId
	 * @return true if successful
	 */
	public boolean setupFacebookQuerier(String siteId) {
		String fbAuthToken = getFacebookOAuthToken(siteId);

		if( fbAuthToken == null ) {
			Util.logError(this, "There are no Facebook credentials in the configuration file to query on behalf of '" + siteId + "'");
			return false;
		}
		Util.logInfo(this, "facebook fbAuthToken" +fbAuthToken);
		
		fb = new FacebookQuerier(fbAuthToken);
		Util.logInfo(this, "Initialized Facebook querier for site '" + siteId + "' using credentials of user '" + fb.fetchUser().getName() + "'" );
		Util.logInfo(this,"Using token for user '" + fb.fetchUser().getName() + "' with value " + fbAuthToken);
		
		monitor = new TopURLSeenByCounterWithLeastRecentlyUsed<FacebookQuerierForURL>(urlsToMonitor, new WorkerFactory<FacebookQuerierForURL>() {

			@Override
			public FacebookQuerierForURL newInstance(String url) {
				return new FacebookQuerierForURL(url, fb);																	
			} } );

		return true;
	}
	
	public void processEvent(URLSeenCounter urlSeenCounter) {
		if ( siteId == null ) {
			siteId = urlSeenCounter.getSite();
			if( ! setupFacebookQuerier(siteId) ) {
				Util.logError(this, "failed to connect to facebook siteId:" +siteId);
				// There was a problem
				return;
			}
			
		} else if( ! siteId.equals(urlSeenCounter.getSite())) {
			Util.logError( this, "There was one PE per site but site changed, was: '" + siteId + "', now it is '" + urlSeenCounter.getSite() + "'");
			return;
		}
		
	//	Util.logDebug(this, "facebook urlSeenCounter :" +urlSeenCounter);
		monitor.add(urlSeenCounter);
	}

	@Override
	public void output() {
		if( monitor == null ) {
			Util.logWarning(this, "No events have been received yet: setupFacebookQuerier() is slow or failed silently..");
			return;
		}
		FacebookQuerierForURL chosenQuerier = monitor.getNext();
		if( chosenQuerier == null ) {
			Util.logWarning(this, "No URLs to query for");
			return;
		}


	        String url = chosenQuerier.getUrl();
		//Util.logDebug(this, "output() querying for " +url );
		
		FacebookURLInfo fbInfo = null;
		try {
			fbInfo = chosenQuerier.query();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			Util.logError(this, "Failed to get infor for the url  " + chosenQuerier.getUrl() );
		}
               
		Util.logDebug(this, "got " + fbInfo.getLike_count() + "  (new) likes " +fbInfo.getShare_count()+"and (new) shares "+ fbInfo.getShare_count() +" for "+ chosenQuerier.getUrl());
		URLSeenFacebook urlSeenFacebook = new URLSeenFacebook(siteId, chosenQuerier.getUrl(), fbInfo.getLike_count(), fbInfo.getShare_count());

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String dateNow = dateFormat.format(date);

		dispatcher.dispatchEvent(outputStreamName, urlSeenFacebook);
	}

	// Simple getters and setters

	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	public void setDispatcher(EventDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public String getOutputStreamName() {
		return outputStreamName;
	}

	public void setOutputStreamName(String outputStreamName) {
		this.outputStreamName = outputStreamName;
	}

	public int getUrlsToMonitor() {
		return urlsToMonitor;
	}

	public void setUrlsToMonitor(int urlsToMonitor) {
		this.urlsToMonitor = urlsToMonitor;
	}

	private String getFacebookOAuthToken(String aSiteId) {
		if( sites.hasSiteId(aSiteId) ) {
			return sites.getSiteById(aSiteId).getFbAuthToken();
		} else {
			return null;
		}
	}
	
	public SiteConfigurations getSites() {
		return sites;
	}

	public void setSites(SiteConfigurations sites) {
		this.sites = sites;
		Util.logInfo(this, "Configured sites: " + sites.listIds());
	}
}
