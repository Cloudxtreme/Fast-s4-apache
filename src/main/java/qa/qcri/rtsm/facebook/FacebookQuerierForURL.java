package qa.qcri.rtsm.facebook;


import org.json.JSONObject;

import qa.qcri.rtsm.persist.cassandra.CassandraPersistentUrlId;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;
import qa.qcri.rtsm.util.LastWorkedTime;
import qa.qcri.rtsm.util.Util;

import com.google.common.collect.ImmutableMap;
import qa.qcri.rtsm.persist.cassandra.CassandraPersistentUrlId;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;
import java.util.HashMap;


public class FacebookQuerierForURL implements LastWorkedTime {

	final String url;
	

	final FacebookQuerier fb;
	
	long lastCheckedTime;
	
        CassandraPersistentUrlId persistentUrlId;

	String id;

	public FacebookQuerierForURL(String url, FacebookQuerier fb) {
		if(url.contains("dohanews.co") && !url.endsWith("/"))
			this.url = url+"/";
		else
			this.url = url;
		this.fb = fb;
		
		this.lastCheckedTime = Long.MIN_VALUE;

                /* 
                 * get thr url ID 
                 */

                persistentUrlId = new CassandraPersistentUrlId();
                persistentUrlId.setColumnFamilyName(CassandraSchema.COLUMNFAMILY_NAME_URL_FB_ID);



	}

	/**
	 * @return number of Facebook shares for the URL
	 * @throws Exception 
	 */
	public FacebookURLInfo query() throws Exception {

		try{
               		this.id = persistentUrlId.get(url, "url");
		}catch(Exception e){}
                Util.logInfo(this,"url:"+url+ " | url_id:"+id);
		FacebookURLInfo result = fb.getURLInfo_new(url, id);
//		result.like_count =101;
//		result.share_count  =200;
		
		this.lastCheckedTime = System.currentTimeMillis();

		if( ! hasWorked() ) {
			throw new IllegalStateException("Just did a query, hasWorked() is returning false" );
		}
		
		return result;
	}
	
	public boolean hasWorked() {
		return (this.lastCheckedTime != Long.MIN_VALUE);
	}

	public long getLastWorkedTime() {
		return lastCheckedTime;
	}
	
	public String getUrl() {
		return url;
	}
	
	public String toString() {
		return (new JSONObject(ImmutableMap.of( "url", url, "fb", fb, "lastCheckedTime", new Long(lastCheckedTime) ))).toString();
	}
}
