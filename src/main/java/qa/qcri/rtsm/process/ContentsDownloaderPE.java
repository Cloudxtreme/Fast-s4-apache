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
import io.s4.processor.AbstractPE;

import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.item.URLSeenParsed;
import qa.qcri.rtsm.util.Util;
import qa.qcri.rtsm.util.WebUtil;

public class ContentsDownloaderPE extends AbstractPE {
	private EventDispatcher dispatcher;

	private String outputStreamName;

	private HashSet<String> urlDownloaded;

	private ConcurrentLinkedQueue<String> urlsToDownload;

	private int httpRequestsByTimeBoundary;

	private int httpRetriesPerURL;
	
	private String site;

	private Map<String,Integer> retriesPerURL;

	@Override
	public void initInstance() {
		super.initInstance();
		site = null;
		urlDownloaded = new HashSet<String>();
		urlsToDownload = new ConcurrentLinkedQueue<String>();
		retriesPerURL = new ConcurrentHashMap<String, Integer>();
	}

	public void processEvent(URLSeenCounter urlSeenCounter) {
		String url = urlSeenCounter.getUrl();
		Util.logDebug(this, "Added url " + url + " to the queue");
		
		if ( site == null ) {
			site = urlSeenCounter.getSite();
		} else if( ! site.equals(urlSeenCounter.getSite())) {
			Util.logError( this, "There was one PE per site but site changed, was: '" + site + "', now it is '" + urlSeenCounter.getSite() + "'");
			return;
		}

		if(retriesPerURL.containsKey(url)) {
			retriesPerURL.put(url,retriesPerURL.get(url)+1);
		}
		else {
			retriesPerURL.put(url, 1);
		}

		// Download URLs that have not been downloaded
		if ( (! urlDownloaded.contains(url)) && (! urlsToDownload.contains(url)) ) {
			// Append to tail of the queue
			urlsToDownload.add(url);
		}
	}

	@Override
	public void output() {

		for (int i = 0; i < httpRequestsByTimeBoundary && urlsToDownload.size() > 0; i++) {
			// Remove from head of the queue
			String url = urlsToDownload.poll();

			// Download and parse
			Util.logDebug(this, "downloading " + url);
			String htmlContents = WebUtil.getHTMLContentsAsString(url);

			if (htmlContents == null || htmlContents.length() == 0) { // if content download failed
				if(retriesPerURL.get(url)<httpRetriesPerURL) {
					retriesPerURL.put(url, retriesPerURL.get(url)+1);
					urlsToDownload.add(url);
					continue;
				}
				else {
					Util.logError( this, "Content dowload for " + url + " failed more than " + httpRetriesPerURL + " times");
					return;
				}
			}

			// Create event and dispatch
			URLSeenParsed urlSeenParsed = new URLSeenParsed(site, url, htmlContents);
			Util.logDebug(this, "Last ContentsDownloader PE invoked 2 transferred data to TopNURLs");
			dispatcher.dispatchEvent(outputStreamName, urlSeenParsed);

			urlDownloaded.add(url);
		}
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

	public int getHttpRequestsByTimeBoundary() {
		return httpRequestsByTimeBoundary;
	}

	public void setHttpRequestsByTimeBoundary(int httpRequestsByTimeBoundary) {
		this.httpRequestsByTimeBoundary = httpRequestsByTimeBoundary;
	}
	
	public int getHttpRetriesPerURL() {
		return httpRetriesPerURL;
	}

	public void setHttpRetriesPerURL(int httpRetriesPerURL) {
		this.httpRetriesPerURL = httpRetriesPerURL;
	}
}
