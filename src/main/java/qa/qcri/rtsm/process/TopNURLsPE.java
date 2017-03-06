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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import qa.qcri.rtsm.item.TimeWindowedSortedQueue;
import qa.qcri.rtsm.item.URLSeenCounter;
import qa.qcri.rtsm.item.URLSeenFacebook;
import qa.qcri.rtsm.item.URLSeenParsed;
import qa.qcri.rtsm.item.URLSeenTweet;
import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.twitter.TwitterTreeAnalyzer;
import qa.qcri.rtsm.util.Util;
import qa.qcri.rtsm.util.WebUtil;

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



public class TopNURLsPE extends PersistedAbstractPE {

	private static final String ATTRIBUTE_TITLE = "title";

	private static final String ATTRIBUTE_OG_IMAGE = "og_image";

	private static final String ATTRIBUTE_FACEBOOK_LIKES = "fb_likes";

	private static final String ATTRIBUTE_FACEBOOK_SHARES = "fb_shares";

	private static final String ATTRIBUTE_MONITORED_SINCE = "monitored_since";

	/**
	 * Element to insert after the basename for the persistence, but before the file associated to
	 * each URL
	 */
	private static final String DIR_INFIX = ".dir/";

	private static final int TWEETS_WINDOW_SIZE_MILLIS = 10000;

	/**
	 * Number of top entries to keep, defined in the configuration file.
	 */
	private int entryCount = -1;

	private Map<String, Integer> urlCountMap;

	private Map<String, Integer> urlVisitsLastSeen;

	private Map<String, Integer> urlFacebookLastSeen;

	private Map<String, Integer> urlTwitterLastSeen;

	private Map<String, JSONObject> urlPropertiesMap;

	private Map<String, TwitterTreeAnalyzer> urlTweetMap;

	private Map<String, TimeWindowedSortedQueue<SimpleTweet>> urlTweetQueueMap;

	private Semaphore addTweetSemaphore;

	private Semaphore addTweetsToQueueSemaphore;

	private Semaphore addVisitSemaphore;

	private Semaphore addFacebookSemaphore;

	private TwitterTreeAnalyzer twitterTreeAnalyzer;

	public int getEntryCount() {
		return entryCount;
	}

	public void setEntryCount(int entryCount) {
		this.entryCount = entryCount;
	}

	private void setProperty(String url, String propertyName, Object propertyValue) {
		if (!urlPropertiesMap.containsKey(url)) {
			urlPropertiesMap.put(url, new JSONObject());
		}
		JSONObject properties = urlPropertiesMap.get(url);
		try {
			properties.put(propertyName, propertyValue);
		} catch (JSONException e1) {
			e1.printStackTrace();
			try {
				properties.put(propertyName, "(invalid property value)");
			} catch (JSONException e2) {
				e2.printStackTrace();
			}
		}
	}

	private String getProperty(String url, String attribute) {
		if (!urlPropertiesMap.containsKey(url)) {
			return null;
		}
		try {
			return urlPropertiesMap.get(url).getString(attribute);
		} catch (JSONException e) {
			return null;
		}
	}

	public void processEvent(URLSeenCounter urlSeenCounter) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		Long longTime = new Long(date.getTime()/1000);
		int currentDate = longTime.intValue();
//		String currentDate = dateFormat.format(date);

		String url = urlSeenCounter.getUrl();
		try {
			addVisitSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		Util.logDebug(this, "Received " + url);
		urlCountMap.put(url, new Integer(urlSeenCounter.getCount()));
		urlVisitsLastSeen.put(url, currentDate);
		addVisitSemaphore.release();
		setProperty(url, ATTRIBUTE_MONITORED_SINCE, new Long(urlSeenCounter.getMonitoredSince()));
	}

	public void processEvent(URLSeenParsed urlSeenParsed) {

		//Util.logTrace(this, "Received " + urlSeenParsed);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String currentDate = dateFormat.format(date);

		String url = urlSeenParsed.getUrl();
		Util.logDebug(this, "Received latest from ContentsDownloaderPE transferred data to " + urlSeenParsed.getOgImage() + " " + urlSeenParsed.getTitle());
		setProperty(url, ATTRIBUTE_TITLE, urlSeenParsed.getTitle());
		setProperty(url, ATTRIBUTE_OG_IMAGE, urlSeenParsed.getOgImage());

	}

	public void processEvent(URLSeenFacebook urlSeenFacebook) {
		
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		Long longTime = new Long(date.getTime()/1000);
		int currentDate = longTime.intValue();
		// String currentDate = dateFormat.format(date);
		/// Util.logDebug(this, "Received latest Facebook PE at " + currentDate);
		
	         //Util.logDebug(this, "Received facebook " + urlSeenFacebook);

		String url = urlSeenFacebook.getUrl();

		try {
			addFacebookSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring Facebook semaphore");
		}

		//if(url.contains("dohanews.co") && url.endsWith("/"))
	          //                 url=    url.substring(0,url.length()-1);
		Util.logDebug(this,"insert url into urlFacebookLastSeen"+ url);
		urlFacebookLastSeen.put(url, currentDate);
		addFacebookSemaphore.release();

		Util.logDebug(this, "Received latest from Facebook PE transferred data to " + urlSeenFacebook.getLikes() + " " + urlSeenFacebook.getShares() + " for url"+url);
		setProperty(url, ATTRIBUTE_FACEBOOK_LIKES, new Long(urlSeenFacebook.getLikes()));
		setProperty(url, ATTRIBUTE_FACEBOOK_SHARES, new Long(urlSeenFacebook.getShares()));

	}

	public void processEvent(URLSeenTweet urlSeenTweet) {

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		Long longTime = new Long(date.getTime()/1000);
		int currentDate = longTime.intValue();
		// String currentDate = dateFormat.format(date);
		// Util.logDebug(this, "Received latest from Twitter PE at " + currentDate);
		

		String url = urlSeenTweet.getUrl();
		Util.logDebug(this, "url:"+ url +"  |  Received tweet " + urlSeenTweet);
		try {
			addTweetSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		// Kader: arabic specific code
		if(url.contains("www.aljazeera.net")  || url.contains("kasra.co") ){
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
		}	
		if (!urlTweetMap.containsKey(url)) {
			Util.logDebug(this,"insert url into urlTweetMap"+ url);
			urlTweetMap.put(url, new TwitterTreeAnalyzer());
			urlTweetQueueMap.put(url, new TimeWindowedSortedQueue<SimpleTweet>(TWEETS_WINDOW_SIZE_MILLIS));
		}

//		SimpleTweet tweet = urlSeenTweet.getSimpleTweet();
//		twitterTreeAnalyzer.addToQueue(tweet);

		SimpleTweet tweet = urlSeenTweet.getSimpleTweet();
		long createdAt = tweet.getCreatedAt().getTime();
		//Util.logDebug(this, "Last twitter PE invoked for tweet created at: " + createdAt + " transferred data to TopNURLs");
		
		Util.logDebug(this,"just before insert url into urlTweetMap "+ url + " | "+tweet);
		urlTweetQueueMap.get(url).insertElement(createdAt, tweet);
		urlTwitterLastSeen.put(url, currentDate);

		//Util.logDebug(this, "urlTwitterLastSeen " + url+" | "+currentDate);
		addTweetSemaphore.release();

	}


	@Override
	public void initInstance() {
		super.initInstance();
		urlCountMap = new ConcurrentHashMap<String, Integer>();
		urlVisitsLastSeen = new ConcurrentHashMap<String, Integer>();
		urlFacebookLastSeen = new ConcurrentHashMap<String, Integer>();
		urlTwitterLastSeen = new ConcurrentHashMap<String, Integer>();

		urlPropertiesMap = new ConcurrentHashMap<String, JSONObject>();
		urlTweetMap = new ConcurrentHashMap<String, TwitterTreeAnalyzer>();
		urlTweetQueueMap = new ConcurrentHashMap<String, TimeWindowedSortedQueue<SimpleTweet>>();
		addTweetSemaphore = new Semaphore(1);
		addTweetsToQueueSemaphore = new Semaphore(1);
		addVisitSemaphore = new Semaphore(1);
		addFacebookSemaphore = new Semaphore(1);
		twitterTreeAnalyzer = new TwitterTreeAnalyzer();
	}

	@Override
	public void output() {
		String site = null;
		if( this.getKeyValue() == null ) {
			Util.logError(this, "this.getKeyValue() is null; skipping output()" );
			return;
		} else if( this.getKeyValue().size() != 1 ) {
			Util.logError(this, "this.getKeyValue().size() != 1; skipping output()" );
			return;
		} else if( ! (this.getKeyValue().get(0) instanceof String )) {
			Util.logError(this, "this.getKeyValue().get(0) is not String; skipping output()" );
			return;
		}
		site = (String) this.getKeyValue().get(0);

		if (entryCount <= 0) {
			Util.logError(this, "The variable entryCount is not defined in the configuration file; skipping output()");
			return;
		}

/*	Util.logInfo(this, "output() start" 
			+ " [entryCount: " + entryCount + "]"
			+ " [urlCountMap.size: " + urlCountMap.size() + "]" 
			+ " [urlPropertiesMap.size: " + urlPropertiesMap.size() + "]" 
			+ " [urlTweetMap.size: " + urlTweetMap.size() + "]" 
			+ " [urlTweetQueueMap.size: " + urlTweetQueueMap.size() + "]" 
			+ " [persister: " + persister + "]" 
			+ " [site: " + site + "]"
			);

*/
		// Incorporate tweets into analyzer
//		Util.logTrace(this, "output() acquiring addTweetsToQueueSemaphore" );
		try {
			addTweetsToQueueSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			Util.logError(this, "Interrupted while acquiring semaphore" );
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}

		/*
		Util.logDebug(this, "output() start TwitterTreeAnalyzer.processQueue calls for " + urlTweetMap.size() + " URLs" );
		for (Entry<String,TwitterTreeAnalyzer> entry: urlTweetMap.entrySet()) {
			String url = entry.getKey();
			String title = getProperty(url, ATTRIBUTE_TITLE);
//			Util.logDebug(this, "DISABLED processQueue");
			twitterTreeAnalyzer.processQueue(title);
		}
		Util.logDebug(this, "output() ends TwitterTreeAnalyzer.processQueue calls");
		*/

		Set<Entry<String, TimeWindowedSortedQueue<SimpleTweet>>> tmp1 = urlTweetQueueMap.entrySet();
		for (Entry<String, TimeWindowedSortedQueue<SimpleTweet>> entry : tmp1) {
			TimeWindowedSortedQueue<SimpleTweet> queue = entry.getValue();
			if (queue.size() > 0) {
				String url = entry.getKey();
				TwitterTreeAnalyzer twitterAnalyzer = urlTweetMap.get(url);
				String title = getProperty(url, ATTRIBUTE_TITLE);
				long lastTweetId = Long.MIN_VALUE;
				SimpleTweet lastTweet = null;
				Vector <SimpleTweet> tmp = queue.flushOldElements();
				Util.logDebug(this, " processing URL " + url + "[urlTweetQueueMap.size: " + tmp1.size() + "], Queue size: " + queue.size() + " flushed " + tmp.size() + " elements");
				int count11 = 0;
				for (SimpleTweet tweet : tmp) {
					
					count11 += 1;
					Util.logTrace(this, "Iteration no. " + count11 + " tweet " + tweet.getText());
					// Check tweetIDs are ascending
					if( tweet.getId() < lastTweetId ) {
						Util.logWarning(this, "in output(), newer tweet has lower tweet-id: " + tweet + " vs " + lastTweet );
					}
					lastTweetId = tweet.getId();
					lastTweet = tweet;
					
					// Add
					Util.logDebug(this, " url from queue  output() adding for '" + url + "' tweet " + tweet);
					if (title != null) {
						twitterAnalyzer.add(tweet, title);
					} else {
						twitterAnalyzer.add(tweet);
					}
				}
			}
		}

		addTweetsToQueueSemaphore.release();

		List<String> urlsSortedByVisitsDesc = new ArrayList<String>(urlCountMap.keySet());

		// Sort by decreasing number of visits
		Collections.sort(urlsSortedByVisitsDesc, new Comparator<String>() {

			@SuppressWarnings("boxing")
			@Override
			public int compare(String url1, String url2) {
				int count1 = urlCountMap.get(url1);
				int count2 = urlCountMap.get(url2);
				if (count1 < count2) {
					return 1;
				} else if (count1 > count2) {
					return -1;
				} else {
					return 0;
				}
			}

		});

		try {
			// List of top visited sites
			JSONArray jsonTopN = new JSONArray();
			int visitsLastSeen = -1, facebookLastSeen = -1, twitterLastSeen = -1;
			for (int i = 0; i < entryCount; i++) {
				if (i == urlsSortedByVisitsDesc.size()) {
					break;
				}
				String url = urlsSortedByVisitsDesc.get(i);
				/*if(url.length()> 200){
					Util.logDebug(this, "URL too long here:"+url);
					continue;
				}
				  kader Sofiane*/
				JSONObject jsonEntry = new JSONObject();
				JSONObject jsonEntry1 = new JSONObject();
				jsonEntry.put("url", url);
				jsonEntry1.put("url", url);
				jsonEntry.put("count", urlCountMap.get(url));
				jsonEntry1.put("count", urlCountMap.get(url));
				
				if(urlVisitsLastSeen.containsKey(url)) {
					visitsLastSeen = urlVisitsLastSeen.get(url);
				}
				else {
					visitsLastSeen = -1;
				}
				if(urlFacebookLastSeen.containsKey(url)) {
					facebookLastSeen = urlFacebookLastSeen.get(url);
				}
				else {
					Util.logDebug(this," urlFacebookLastSeen does not contain url  "+ url);
					facebookLastSeen = -1;
				}
				if(urlTwitterLastSeen.containsKey(url)) {
					twitterLastSeen = urlTwitterLastSeen.get(url);
				}
				else {
					twitterLastSeen = -1;
				}

				jsonEntry.put("visits_last_seen",visitsLastSeen);
				jsonEntry1.put("visits_last_seen",visitsLastSeen);
				jsonEntry.put("facebook_last_seen",facebookLastSeen);
				jsonEntry1.put("facebook_last_seen",facebookLastSeen);
				jsonEntry.put("twitter_last_seen",twitterLastSeen);
				jsonEntry1.put("twitter_last_seen",twitterLastSeen);

				// Add properties
				if (urlPropertiesMap.containsKey(url)) {
					JSONObject urlProperties = urlPropertiesMap.get(url);
					@SuppressWarnings("unchecked")
					Iterator<String> urlPropertiesIterator = urlProperties.keys();
					while (urlPropertiesIterator.hasNext()) {
						String key = urlPropertiesIterator.next();
						// Forces everything to a string
						jsonEntry.put(key, urlProperties.getString(key));
						jsonEntry1.put(key, urlProperties.getString(key));
					}
				}

				try {
				// Add tweets
				if (urlTweetMap.containsKey(url)) {
					Util.logDebug(this,"urlTweetMap must contain " + url);
					JSONObject tweets = urlTweetMap.get(url).toJSONObject();
					//Util.logDebug(this,"TWEETS " + tweets.toString());
					jsonEntry.put("tweets", tweets);
					jsonEntry1.put("count_descendants", tweets.get("count-descendants"));
				}else
					Util.logDebug(this,"urlTweetMap does not contain " + url);
				} catch (JSONException e) {
					Util.logError(this,"ERROR in accessing json tweets " + e.toString());
					return;
				}
				
//				jsonEntry.put("tweets","");
//				jsonEntry1.put("count_descendants",0);

				// Persist information for each URL on a different file
				Util.logDebug(this,"tweet into file:"+site + DIR_INFIX + WebUtil.urlToFilename(url));
				Util.logDebug(this,"json:"+jsonEntry.toString());
				//safePersist(site + DIR_INFIX + WebUtil.urlToFilename(url), jsonEntry.toString()); kader
				safePersist(site + DIR_INFIX + WebUtil.urlToFilename1(url), jsonEntry.toString());

				// Add information about the URL to the file
				jsonTopN.put(jsonEntry1);
//				jsonTopN.put(jsonEntry);
			}

		//	Util.logTrace(this, "output() ends write to individual files, will write joint file" );

			// Rest of JSON message
			JSONObject message = new JSONObject();
			message.put("host", site);
			message.put("entry-count", entryCount);
			message.put("top", jsonTopN);
			Util.logDebug(this, " write to file "+site);
			safePersist(site, message.toString() + "\n");
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (Exception e) {
			Util.logError(this,"ERROR " + e.toString());
			e.printStackTrace();
		}

		Util.logTrace(this, "output() ended normally" );
	}

	void safePersist(String filename, String message ) {
		if( persistTime <= 0 ) {
			Util.logError(this, "persistTime is not positive: "  + persistTime );
		} else if( filename == null || filename.length() == 0 ) {
			Util.logError(this, "filename is null or empty: '"  + filename + "'" );
		} else if( message == null || message.length() == 0 ) {
			Util.logError(this, "message is null or empty: '"  + message + "'" );
		} else {
			Util.logDebug(this, "filename: '" + filename  );
			try {
				persister.set(filename, message, persistTime);
			} catch(InterruptedException e) {
				Util.logWarning(this, "Persister.set threw interrupted exception");
			}
		}
	}
}
