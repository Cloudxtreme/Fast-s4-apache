package qa.qcri.rtsm.facebook;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import qa.qcri.rtsm.persist.cassandra.CassandraPersistentUrlId;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;
import qa.qcri.rtsm.util.Util;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.Version;
import com.restfb.exception.FacebookException;
import com.restfb.exception.FacebookOAuthException;
import com.restfb.json.JsonObject;
import com.restfb.types.User;





/**
 * 
 * @author Imran
 */
public class FacebookQuerier {

	private  FacebookClient fbClient;
	
	private final String accessToken;
	
	
	public FacebookQuerier(String accessToken)  {
		try{
			fbClient = new DefaultFacebookClient(accessToken,	Version.VERSION_2_8);
			}catch(Exception s){s.printStackTrace();}

		this.accessToken = accessToken;
		Util.logInfo(this, " passed connection here ..");
		 
		
	}
	
	public User fetchUser() {
		return fetchUser("me");
	}
	
	public User fetchUser(String username) {
		try {
		
			return fbClient.fetchObject(username, User.class);
		} catch( FacebookException e ) {
			Util.logError(this, "Got exception: " + e + " while using token '" + accessToken + "'");
			if( e instanceof FacebookOAuthException ) {
				Util.logError(this, "Exception has error code: " + ((FacebookOAuthException)e).getErrorCode() );
			}
			return null;
		}
	}
	
	public FacebookURLInfo getURLInfo(String url) {
		String query = "SELECT click_count, like_count, share_count, comment_count, commentsbox_count, total_count FROM link_stat WHERE url=\"" + url + "\"";
		List<FacebookURLInfo> pageinfo = fbClient.executeQuery(query, FacebookURLInfo.class);
		if( pageinfo == null ) {
			Util.logWarning(this, "Query returned null: " + query);
			return null;
		} else if( pageinfo.size() != 1 ) {
			Util.logWarning(this, "Query returned " + pageinfo.size() + " results: " + query);
			return null;
		} else {
			return pageinfo.get(0);
		}
	}
	
	public FacebookURLInfo getURLInfo_new(String url, String id)  {
	

	try{	

		List<String> ids = new ArrayList<String>();
		ids.add(id);
		JsonObject results= fbClient.fetchObjects(ids, JsonObject.class,Parameter.with("fields", "likes.summary(true),shares,link"));
		//Util.logDebug(this,"results:" + results.toString());
		
		if (results ==null){
			Util.logWarning(this,"Query returned null: " + results);
			return null;
		}
		
	
		Iterator itr = results.keys();
		FacebookURLInfo pageinfo = new FacebookURLInfo();
		while (itr.hasNext()) {
			String key = (String) itr.next();
			JsonObject value = results.getJsonObject(key);
			JsonObject likes = (JsonObject) (new JsonObject(value.getString("likes")).get("summary"));
			String likes_count = likes.get("total_count").toString();
			try {
				String link = generateUrlFromShortUrl(value.getString("link"));
			} catch (Exception e) {
				Util.logWarning(this, "No id for  url "+url);
			}
			String shares_count =  (new JsonObject(value.getString("shares")).get("count")).toString();
			pageinfo.like_count = Long.parseLong(likes_count);
			pageinfo.share_count = Long.parseLong(shares_count);			

		}
		//Util.logInfo(this,"pageinfo:"+pageinfo);
		return pageinfo;
	}catch(Exception e){ return null;}
		
	}
  	/*
  	 * read form facebook.com the equivalent URL to the shortend URL
  	 * 
  	 */
    public static String generateUrlFromShortUrl(String link ) throws Exception {
	    final URL url = new URL(link);
	    final HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
	    urlConnection.setInstanceFollowRedirects(true);
	    final String location = urlConnection.getHeaderField("location");
	    return urlConnection.getURL().toString();
	}	

}
