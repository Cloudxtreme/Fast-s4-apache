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

import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import qa.qcri.rtsm.item.SiteConfigurations;
import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.item.URLSeenTweet;
import qa.qcri.rtsm.twitter.TwitterSearcher;
import qa.qcri.rtsm.twitter.TwitterSearcher.TwitterSearcherInfo;
import qa.qcri.rtsm.twitter.TwitterSearcherForURL;
import qa.qcri.rtsm.util.TopURLSeenByCounterWithLeastRecentlyUsed;
import qa.qcri.rtsm.util.TopURLSeenByCounterWithLeastRecentlyUsed.WorkerFactory;
import qa.qcri.rtsm.util.Util;
import twitter4j.Status;
import twitter4j.TwitterException;

/**
 * This PE keeps a set of "active" URLs and perform periodic searches for them.
 * 
 * @author chato
 * 
 */
public class TwitterSearcherPE extends LoggableAbstractPE {

	private EventDispatcher dispatcher;

	private String outputStreamName;

	private int urlsToMonitor;

	private TwitterSearcher ts;

	private String twitterConsumerKey;

	private String twitterConsumerSecret;

	private TopURLSeenByCounterWithLeastRecentlyUsed<TwitterSearcherForURL> monitor;

	private SiteConfigurations sites;
	
	private String siteId;
	
	@Override
	public void initInstance() {
		super.initInstance();
		siteId = null;
		ts = null;
		monitor = null;
	}

	public boolean setupTwitterSearcher(String siteId) {
		if (!sites.hasSiteId(siteId)) {
			Util.logError(this, "There are no Twitter credentials in the configuration file to query on behalf of '" + siteId + "'");
			return false;
		}

		Util.logTrace(this, "Creating twitter searcher object for '" + siteId + "'");
		ts = new TwitterSearcher(getTwitterConsumerKey(), getTwitterConsumerSecret(), getTwitterOAuthAccessToken(siteId), getTwitterOAuthAccessTokenSecret(siteId));
		
		//Util.logInfo(this, "twitter api keys:"+siteId+"|"+getTwitterConsumerKey()+"|"+getTwitterConsumerSecret()+"|"+getTwitterOAuthAccessToken(siteId)+"|"+ getTwitterOAuthAccessTokenSecret(siteId) );
		//Util.logInfo(this, "Getting twitter info for " + ts);
		TwitterSearcherInfo info;
		try {		
			info = ts.getInfo();
		} catch( TwitterException e ) {
			Util.logError(this, "There was a problem getting info for the twitter searcher");
			e.printStackTrace();
			return false;
		}
		
		Util.logDebug(this, "Initialized twitter searcher for '" + siteId + "' with credentials of '" + info.getScreenName() + "', limit is " + info.getLimit() );
		
		Util.logTrace(this, "Creating monitor");
		monitor = new TopURLSeenByCounterWithLeastRecentlyUsed<TwitterSearcherForURL>(urlsToMonitor, new WorkerFactory<TwitterSearcherForURL>() {

			@Override
			public TwitterSearcherForURL newInstance(String url) {
				return new TwitterSearcherForURL(url, ts);
			}
		});

		return true;
	}

	public void processEvent(URLSeenCounter urlSeenCounter) {
		
		if ( siteId == null ) {
			siteId = urlSeenCounter.getSite();

			Util.logDebug(this, "Setting up twitter searcher for '" + siteId + "'");
			if( ! setupTwitterSearcher(siteId) ) {
				// There was a problem
				Util.logError(this, "Twitter searcher could not be set-up for '" + siteId + "'");
				return;
			}
			
		} else if( ! siteId.equals(urlSeenCounter.getSite())) {
			Util.logError( this, "There was one PE per site but site changed, was: '" + siteId + "', now it is '" + urlSeenCounter.getSite() + "'");
			return;
		}
		monitor.add(urlSeenCounter);
	}

	@Override
	public void output() {
		if (monitor == null ) {
			Util.logWarning(this, "No events have been received yet: setupTwitterSearcher() is slow or failed silently");
			return;
		}
		
		TwitterSearcherForURL chosenSearcher = monitor.getNext();
		if (chosenSearcher == null) {
			Util.logWarning(this, "No URLs to search for");
			return;
		}

	//	Util.logDebug(this, "output() searching for " + chosenSearcher.getUrl());
		List<Status> results = chosenSearcher.search();
		Util.logDebug(this, "Received output() got " + results.size() + " (new) results for " + chosenSearcher.getUrl() + " now hasSearched()==" + chosenSearcher.hasWorked());
		
		// Dispatch in reverse order (older first) and check the IDs are ascending
		long lastID = Long.MIN_VALUE;
		for (int i = results.size() - 1; i >= 0; i--) {
			Status tweet = results.get(i);

			// Check IDs are ascending
			long tweetID = tweet.getId();
			if (tweetID <= lastID) {
				throw new IllegalStateException("Tweets were not sorted from newer to older");
			}
			lastID = tweetID;
			String url_ = chosenSearcher.getUrl();
			URLSeenTweet urlSeenTweet = new URLSeenTweet(siteId, url_, tweet);
			dispatcher.dispatchEvent(outputStreamName, urlSeenTweet);
			//Util.logDebug(this, "Dispatched: " + url_);
//			Util.logDebug(this, "Received tweet Dispatched: " + urlSeenTweet.toString());
		}
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String dateNow = dateFormat.format(date);
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

	public String getTwitterConsumerKey() {
		return twitterConsumerKey;
	}

	public void setTwitterConsumerKey(String twitterConsumerKey) {
		this.twitterConsumerKey = twitterConsumerKey;
	}

	public String getTwitterConsumerSecret() {
		return twitterConsumerSecret;
	}

	public void setTwitterConsumerSecret(String twitterConsumerSecret) {
		this.twitterConsumerSecret = twitterConsumerSecret;
	}

	public SiteConfigurations getSites() {
		return sites;
	}

	public void setSites(SiteConfigurations sites) {
		this.sites = sites;
	}

	private String getTwitterOAuthAccessToken(String aSiteId) {
		if (sites.hasSiteId(aSiteId)) {
			return sites.getSiteById(aSiteId).getTwitterAuthToken();
		} else {
			return null;
		}
	}

	private String getTwitterOAuthAccessTokenSecret(String aSiteId) {
		if (sites.hasSiteId(aSiteId)) {
			return sites.getSiteById(aSiteId).getTwitterAuthTokenSecret();
		} else {
			return null;
		}
	}

	public int getUrlsToMonitor() {
		return urlsToMonitor;
	}

	public void setUrlsToMonitor(int urlsToMonitor) {
		this.urlsToMonitor = urlsToMonitor;
	}
}
