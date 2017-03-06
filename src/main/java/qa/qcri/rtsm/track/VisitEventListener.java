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
package qa.qcri.rtsm.track;

import io.s4.client.Driver;
import io.s4.client.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.json.JSONObject;
import org.simpleframework.http.Form;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.simpleframework.util.thread.Scheduler;

import qa.qcri.rtsm.util.Util;
import qa.qcri.rtsm.item.Visit;
import qa.qcri.rtsm.util.WebUtil;

public class VisitEventListener implements Container {

	final static int HTTP_PORT = 8080;
	final static String S4_HOST = "localhost";
	

	final static int S4_PORT = 2334;
	// This port translates to 8084 on scpro3

	final static int NUM_THREADS = 10;
	
	public final static String KEY_SITE_ID = "owa_site_id";
	
	public final static String KEY_URL = "owa_page_url";
	
	public final static String KEY_VISITOR_ID = "owa_visitor_id";
	
	public final static String KEY_SOURCE = "owa_source";
	
	public final static String KEY_SEARCH_TERMS = "owa_search_terms";
	
	public final static String KEY_REFERRAL = "owa_HTTP_REFERER";
	
	final static String KEY_SAMPLE_RATE = "owa_cv1";
	
	final static String SUBKEY_SAMPLE_RATE = "sampleRate";
	
    final static String STREAM_NAME = "RawVisit";
	
	Scheduler inQueue;

    static LinkedBlockingQueue<Visit> outQueue;

	public static Driver driver;
	public static Driver driver_scpro3;
    
	public VisitEventListener(Scheduler aInQueue, LinkedBlockingQueue<Visit> aOutQueue) {
		this.inQueue = aInQueue;
		outQueue = aOutQueue;
		
        // Start thread to dispatch events to S4
        (new Thread(new Dequeuer())).start();
	}
	
	class Dequeuer implements Runnable {
           public void run() {
            while (!Thread.interrupted()) {
                try {
                    Visit visit = outQueue.take();
                    JSONObject visitJSON = new JSONObject(visit);
                    Message m = new Message(STREAM_NAME, Visit.class.getName(), visitJSON.toString() );
                    boolean success = driver.send(m);
                    System.out.println( (success ?  "OK " : "ERROR ") + STREAM_NAME + " " + Visit.class.getName() + " " + visitJSON );
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.out.println( "ERROR " + STREAM_NAME + " " + ie.getClass().getSimpleName()  );
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.out.println( "ERROR " + STREAM_NAME + " " + ioe.getClass().getSimpleName() + " (is S4 running?)"  );
		}
            }  
          }
	}

	public void handle(Request request, Response response) {
		// Send a response immediately and close the connection
		EmptyResponse.sendEmptyResponse(request, response);
		
		// Start an asynchronous task to process this request
		ProcessRequest task = new ProcessRequest(request);
		inQueue.execute(task);
	}
	
	public static class ProcessRequest implements Runnable {

		private final Request request;

		public ProcessRequest(Request request) {
			this.request = request;
		}

		public void run() {
			
			// Parse form
			Form form;
			try {
				form = request.getForm();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			InetSocketAddress ipAddress = request.getClientAddress();
			String IPAddress = ipAddress.toString();
			Util.logDebug(this, "IP Address is: " + IPAddress);
			// Mandatory parameters: url and siteID
			String url = form.get(KEY_URL);
			if( url == null || url.length() == 0 ) {
				System.out.println("ERROR " + KEY_URL + " parameter missing" );
				return;
			}
			String siteID = form.get(KEY_SITE_ID);
			if( siteID == null || siteID.length() == 0 ) {
				System.out.println("ERROR " + KEY_SITE_ID + " parameter missing" );
				return;
			}
			if( ! WebUtil.checkURLHostContains(url, siteID) ) {
				System.out.println("ERROR The site id: '" + siteID + "' is not contained in the host of URL '" + url + "'" );
				return;
			}
                      // if(url.contains("dohanews.co") && url.endsWith("/"))
                      //         url=    url.substring(0,url.length()-1);
			// Optional parameters
			String source = form.get(KEY_SOURCE);
			String searchTerms = form.get(KEY_SEARCH_TERMS);
			String referral = form.get(KEY_REFERRAL);
			String visitorID = form.get(KEY_VISITOR_ID);
			double sampleRate = form.containsKey(KEY_SAMPLE_RATE) ? getDoubleSubKey( form.get(KEY_SAMPLE_RATE), SUBKEY_SAMPLE_RATE, 1.0 ) : 1.0;
			long timestamp = System.currentTimeMillis();
			
			// Create object and set parameters
			Visit visit = new Visit();
			visit.setSiteID(siteID);
			visit.setSource(source);
			visit.setSearchTerms(searchTerms);
			visit.setReferral(referral);
			visit.setVisitorID(visitorID);
			visit.setUrl(url);
			visit.setSampleRate(sampleRate);
			visit.setTimestamp(timestamp);
			visit.setIPAddress(IPAddress);
			
			outQueue.add(visit);
		}
		
		private double getDoubleSubKey( String value, String subKey, double defaultValue ) {
			List<NameValuePair> list = new ArrayList<NameValuePair>();
			URLEncodedUtils.parse( list, new Scanner(value), "UTF-8" );
			for( NameValuePair pair: list ) {
				if( pair.getName().equals(subKey) ) {
					return Double.valueOf(pair.getValue()).doubleValue();
				}
			}
			return defaultValue;
		}
	}
	
	public static void main(String[] list) throws Exception {
		// Initialize S4 client
		driver = new Driver(S4_HOST, S4_PORT);
		if( driver == null ) {
			throw new RuntimeException("Couldn't communicate with S4 instance in " + S4_HOST + ":" + S4_PORT + "\n" + "driver == null");
		}
		
		try {
		    boolean init = driver.init();
		    init &= driver.connect();
		    if (!init) {
			throw new RuntimeException("Couldn't communicate with S4 instance in " + S4_HOST + ":" + S4_PORT + "\n" + "!init");
		    }
		} catch (IOException ioe) {
		    throw new RuntimeException("Couldn't communicate with S4 instance in " + S4_HOST + ":" + S4_PORT + "\n" + ioe ); 
		} catch (NullPointerException npe) {
			throw new RuntimeException("Couldn't communicate with S4 instance in " + S4_HOST + ":" + S4_PORT + "\n" + npe );
		}
		
		System.out.println("Connected to S4 on " + S4_HOST + ":" + S4_PORT );
		// Connecting to S4 on SCPRO3 - MEEZA
		//driver_scpro3 = new Driver(S4_HOST_scpro3, S4_PORT_scpro3);
		/*if( driver_scpro3 == null ) {
			throw new RuntimeException("Couldn't communicate with S4 instance in " + S4_HOST_scpro3 + ":" + S4_PORT_scpro3 + "\n" + "driver == null");
		}*/
		/*if(driver_scpro3 != null){
			try {
			    boolean init_scpro3 = driver_scpro3.init();
			    init_scpro3 &= driver_scpro3.connect();
			    if (!init_scpro3) {
				System.out.println("Couldn't communicate with S4 instance in " + S4_HOST_scpro3 + ":" + S4_PORT_scpro3 + "\n" + "!init");
			    }
			} catch (IOException ioe) {
			    System.out.println("Couldn't communicate with S4 instance in " + S4_HOST_scpro3 + ":" + S4_PORT_scpro3 + "\n" + ioe ); 
			} catch (NullPointerException npe) {
				System.out.println("Couldn't communicate with S4 instance in " + S4_HOST_scpro3 + ":" + S4_PORT_scpro3 + "\n" + npe );
			}
			System.out.println("Connected to S4 on " + S4_HOST_scpro3 + ":" + S4_PORT_scpro3 );
		}
*/
		// Start web server
		Scheduler theInQueue = new Scheduler(NUM_THREADS);
	    	LinkedBlockingQueue<Visit> theOutQueue = new LinkedBlockingQueue<Visit>();
		Container container = new VisitEventListener(theInQueue,theOutQueue);
		Connection connection = new SocketConnection(container);
		SocketAddress address = new InetSocketAddress(HTTP_PORT);
		
		System.out.println("Will listen for HTTP connections on port " + HTTP_PORT );

		connection.connect(address);
	}
}
