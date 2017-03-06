package qa.qcri.rtsm.twitter;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;

import qa.qcri.rtsm.util.Util;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import com.google.common.collect.ImmutableMap;
import qa.qcri.rtsm.util.Util;
public class TwitterSearcher {
	
	final Twitter twitter;
	
	public TwitterSearcher() {
		TwitterFactory factory = new TwitterFactory();
	    twitter = factory.getInstance();
	}
	
	public TwitterSearcher(String consumerKey, String consumerSecret, String oAuthAccessToken, String oAuthAccessTokenSecret ) {
		this();
	    AccessToken accessToken = new AccessToken(oAuthAccessToken, oAuthAccessTokenSecret);
	    twitter.setOAuthConsumer(consumerKey, consumerSecret);
	    twitter.setOAuthAccessToken(accessToken);
	}

	@Override
	public String toString() {
		return (new JSONObject(ImmutableMap.of( "twitter", twitter ))).toString();
	}
	
	public static class TwitterSearcherInfo {
		final String screenName;
		final int limit;
		
		TwitterSearcherInfo(Twitter twitter) throws TwitterException {
			screenName = twitter.getScreenName();
			Map<String, RateLimitStatus> rls = twitter.getRateLimitStatus("search");
			if( rls == null ) {
				throw new IllegalStateException("twitter.getRateLimitStatus(\"search\") returned null");
			}
			
			RateLimitStatus rlsSearchTweets = rls.get("/search/tweets");
			if( rlsSearchTweets == null ) {
				throw new IllegalStateException("twitter.getRateLimitStatus(\"search\").get(\"search/tweets\") returned null");
			}
			limit = rlsSearchTweets.getLimit();
		}

		public String getScreenName() {
			return screenName;
		}

		public int getLimit() {
			return limit;
		}
	}
	
	public TwitterSearcherInfo getInfo() throws TwitterException {
		return new TwitterSearcherInfo(twitter);
	}
	
	public class TwitterSearchResults {
		final TreeSet<Status> tweets;
		
		final static int MAX_TWEETS_PER_PAGE = 100;
		final static int MAX_TWEETS_PER_SEARCH = 1000;
		
		TwitterSearchResults(final Query initialQuery) {
			QueryResult result = null;

			tweets = new TreeSet<Status>();
			
			Query query = initialQuery;
			query.count(MAX_TWEETS_PER_PAGE);
			
			do {
				
				try {
					// Issue the search
				//	Util.logDebug(this, "query: " + query);				
					result = twitter.search(query);
					List<Status> newTweets = result.getTweets();
					
					Util.logDebug(this, "Returned " + newTweets.size() + " tweets");
					
					// Check that there are no duplicates
					for( Status newTweet: newTweets ) {
						if( tweets.contains(newTweet) ) {
							Util.logDebug(this, "Warning: skipping duplicate tweet from the API for query " + query); 
						} else {
							tweets.add(newTweet);
						}
					}
					
					// If necessary, visit next page
					if( result.hasNext() ) {
						query = result.nextQuery();
					} else {
						query = null;
					}
					
				} catch (TwitterException e) {
					if( e.exceededRateLimitation() ) {
						Util.logError(this, "Exceeded rate limits" );
					}
					e.printStackTrace();
					query = null;
				}
				
			} while( query != null && tweets.size() <= MAX_TWEETS_PER_SEARCH );
		}
		
		Vector<Status> getTweets() {
			return new Vector<Status>(tweets);
		}
	}
	
	/**
	 * @param queryString
	 * @return a list of tweets, with the newest tweet on top
	 */
	public synchronized TwitterSearchResults search(String queryString) {
		Query query = new Query(queryString);
		query.setResultType(Query.RECENT);
		return new TwitterSearchResults(query);
	}
	
	public synchronized TwitterSearchResults search(String queryString, long sinceID) {
		Query query = new Query(queryString);
		query.setResultType(Query.RECENT);
		query.setSinceId(sinceID);
		return new TwitterSearchResults(query);
	}
}
