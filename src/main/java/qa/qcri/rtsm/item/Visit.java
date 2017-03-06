/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package qa.qcri.rtsm.item;

import java.net.MalformedURLException;
import java.net.URL;

import qa.qcri.rtsm.util.WebUtil;

/**
 * @author chato
 *
 */
public class Visit {

	// Mandatory parameters
	private String siteID;
	private String url;
	private String URLPattern;
	
	// Optional parameters
	private String source;
	private String searchTerms;
	private String referral;
	private String visitorID;
	private double sampleRate;
	private long timestamp;
	private String IPAddress;
	
    public Object clone() {
        try {
            Object clone = super.clone();
            return clone;
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }
    }
    
	public String getSiteID() {
		return siteID;
	}

	public void setSiteID(String siteID) {
		if( siteID == null || siteID.length() == 0 ) {
			throw new IllegalArgumentException("Empty siteID");
		}
		this.siteID = siteID;
	}
	
	public String getURLPattern() {
		return URLPattern;
	}
	
	public void setURLPattern(String URLPattern) {
		if( URLPattern != null)
			this.URLPattern = URLPattern;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		if( url == null || url.length() == 0 ) {
			throw new IllegalArgumentException("Empty url");
		}
		try {
			this.url = (new URL(url)).toString();
		} catch (MalformedURLException e) {
			this.url = "malformed-url-exception-at-VisitSetUrl";
		}
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSearchTerms() {
		return searchTerms;
	}

	public void setSearchTerms(String searchTerms) {
		this.searchTerms = searchTerms;
	}
	
	public String getReferral() {
		return referral;
	}

	public void setReferral(String referral) {
		this.referral = referral;
	}

	public String getVisitorID() {
		return visitorID;
	}

	public void setVisitorID(String visitorID) {
		this.visitorID = visitorID;
	}

	public double getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getIPAddress() {
		return IPAddress;
	}

	public void setIPAddress(String IPAddress) {
		this.IPAddress = IPAddress;
	}

	/**
	 * Strips the 'query' and 'fragment' parts from the URL, and normalizes using the normalize() method in java.net.URI
	 * @return
	 */
	public String getStrippedNormalizedUrl() {
		return WebUtil.getStrippedNormalizedUrl(this.url);
	}
}
