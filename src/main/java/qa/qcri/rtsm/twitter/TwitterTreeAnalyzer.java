package qa.qcri.rtsm.twitter;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import qa.qcri.rtsm.analysis.TimeSeries;
import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.item.TimeWindowedSortedQueue;
import qa.qcri.rtsm.util.RTSMConf;
import qa.qcri.rtsm.util.Util;
import twitter4j.Status;
import twitter4j.User;

import com.twitter.Extractor;
import qa.qcri.rtsm.util.Util;

/**
 * @author chato
 * 
 *         TODO: implement bounds (use LinkedBlockingDequeue)
 */
public class TwitterTreeAnalyzer {
	final ConcurrentLinkedQueue<Node> tweets;

	final Semaphore addTweetSemaphore;

	final Semaphore addTweetQueueSemaphore;

	long latestID = Long.MIN_VALUE;

	private Node root = null;

	private static final int MIN_DISPLAY_NOVELTY_CHARACTERS = 8;

	private static final double MAX_DISTANCE_FRACTION_LENGTH_ASSOCIATE_ROOT = 0.3;

	final HashSet<Long> seenTweetID;

	final ConcurrentHashMap<Long, Double> opinionScore;

	final TwitterOpinionQuerier twitterOpinionQuerier;

	boolean twitterOpinionQuerierEnabled;
	
	boolean blacklistFlag;
	
	final Blacklist blacklist;

	boolean twitterBlacklistFlaggerEnabled;
	
	TweetLanguageDetection tweetLanguage;
	
	String tweetLanguageGoogle = null;
	
	boolean tweetLanguageDetectorEnabled;
	
	String tweetDates = null;
	
	String tweetLocations = null;
	
	String tweetMentions = null;
	
	String tweetHashtags = null;
	
	String tweetNames = null;
	
	boolean tweetNLPFeaturesEnabled;

	// private Map<String, TimeWindowedSortedQueue<SimpleTweet>> urlTweetQueueMap;
	private TimeWindowedSortedQueue<SimpleTweet> urlTweetQueueMap;

	private static final int TWEETS_WINDOW_SIZE_MILLIS = 10000;
	
	public static class Node {
		final Status tweet;

		/**
		 * The parent of this node (most similar tweet that came before).
		 */
		final Node parent;

		/**
		 * The number of unique characters in this tweet, wrt its parent.
		 */
		final private int novelty;

		/**
		 * The text of the unique part of this tweet.
		 */
		final private String diff;

		/**
		 * The children of this node (similar tweets that came afterwards).
		 */
		final Vector<Node> children;
		
		/**
		 * True iff this is a retweet
		 */
		final boolean isRetweet;

		/**
		 * The total number of descendants of this tweet.
		 */
		int countDescendants;

		public Node(Status tweet, Node parent, int novelty, String diff, boolean isRetweet) {
			this.tweet = tweet;
			this.parent = parent;
			if (parent != null) {
				// Prevent loops
				if (parent.tweet.getId() == this.tweet.getId()) {
					throw new IllegalArgumentException("Tweet has same ID as parent");
				}

				// Add a new child to the parent
				parent.addChild(this);
			}
			this.novelty = novelty;
			this.diff = StringUtils.replace(diff, "\n", "");
			this.children = new Vector<Node>();
			this.countDescendants = 0;
			this.isRetweet = isRetweet;
		}

		public Status getTweet() {
			return tweet;
		}

		public void addChild(Node node) {
			this.children.add(node);
			countOneMoreDescendant();
		}

		public void countOneMoreDescendant() {
			countDescendants++;
			if (parent != null) {
				parent.countOneMoreDescendant();
			}
		}

		public String toString() {
			return novelty + " '" + diff + "' :: " + tweet.getText() + " :: " + (parent == null ? "ROOT" : parent.getTweet().getText());
		}

		public Vector<Node> getChildren() {
			return this.children;
		}

		public int getCountDescendants() {
			return this.countDescendants;
		}

		public int getNovelty() {
			return novelty;
		}

		public boolean hasChildren() {
			return( this.children.size() > 0 );
		}

		public boolean isRetweet() {
			return this.isRetweet;
		}
	}

	public TwitterTreeAnalyzer() {
		tweets = new ConcurrentLinkedQueue<Node>();
		latestID = Long.MIN_VALUE;
		addTweetSemaphore = new Semaphore(1);
		addTweetQueueSemaphore = new Semaphore(1);
		seenTweetID = new HashSet<Long>();
		opinionScore = new ConcurrentHashMap<Long, Double>();
		twitterOpinionQuerier = new TwitterOpinionQuerier(RTSMConf.TWITTER_OPINION_HOST, RTSMConf.TWITTER_OPINION_PORT);
		twitterOpinionQuerierEnabled = true;
		blacklistFlag = false;
		blacklist = new Blacklist();
		twitterBlacklistFlaggerEnabled = true;
		tweetLanguageGoogle = "";
		tweetLanguage = new TweetLanguageDetection();
		tweetLanguageDetectorEnabled = true;
		tweetDates = "";
		tweetLocations = "";
		tweetMentions = "";
		tweetHashtags = "";
		tweetNames = "";
		tweetNLPFeaturesEnabled = true;
//		urlTweetQueueMap = new ConcurrentHashMap<String, TimeWindowedSortedQueue<SimpleTweet>>();
		urlTweetQueueMap = new TimeWindowedSortedQueue<SimpleTweet>(TWEETS_WINDOW_SIZE_MILLIS);
		
		Util.setLogLevel(this,Level.DEBUG);
	}

	public TwitterTreeAnalyzer(Collection<Status> tweets) throws InterruptedException {
		this();
		for (Status tweet : tweets) {
			this.add(tweet);
		}
	}
	
	public void disableTwitterOpinionQuerier() {
		twitterOpinionQuerierEnabled = false;
	}
	
	public void disableTwitterBlacklistFlagger() {
		twitterBlacklistFlaggerEnabled = false;
	}

	public void disableTweetLanguageDetector() {
		tweetLanguageDetectorEnabled = false;
	}

	public void disableTweetNLPFeatures() {
		tweetNLPFeaturesEnabled = false;
	}
	
	static class ParentAndDifference {
		final public Node parent;

		final public int novelty;

		final public String diff;
		
		final public boolean isRetweet;

		public ParentAndDifference(Node parent, int novelty, String diff, boolean isRetweet) {
			this.parent = parent;
			this.novelty = novelty;
			this.diff = diff;
			this.isRetweet = isRetweet;
		}
	}

//	public void add(Status tweet, String title, int count) {
	public void add(Status tweet, String title) {
		add(tweet);

/*
has been commented out on March, 2017, to solve the bug, this is temporarely solution, more investigation needed here!

		if( twitterOpinionQuerierEnabled ) {
			double score = twitterOpinionQuerier.query(tweet.getText(), title);
			opinionScore.put(new Long(tweet.getId()), new Double(score));
//			Util.logDebug(this, "Opinion score for tweet " + tweet.getText() + "' title '" + title + "' = " + score + " Iteration: " + count);
			Util.logTrace(this, "Opinion score for tweet " + tweet.getText() + "' title '" + title + "' = " + score);
		}
		if( twitterBlacklistFlaggerEnabled ) {

			String blacklistString = blacklist.tweetContainsBlacklistTermString(tweet.getText(), blacklist.blacklistWords);
			blacklistFlag = blacklist.tweetContainsBlacklistTerm(tweet.getText(),blacklist.blacklistWords);
			if(blacklistFlag) {
				// Util.logDebug(this, "Tweet contains blacklisted words "+blacklistString + " Iteration: " + count);
				Util.logTrace(this, "Tweet contains blacklisted words "+blacklistString);
			}
			else {
				// Util.logDebug(this, "Tweet doesn't contain blacklisted words Iteration: " + count);
				Util.logTrace(this, "Tweet doesn't contain blacklisted words");
			}
			
		}
		if( tweetLanguageDetectorEnabled ) {
			tweetLanguageGoogle = tweetLanguage.getLanguage(tweet.getText());
			// Util.logDebug(this, "Tweet language: " + tweet.getText() + "\t" + tweetLanguageGoogle + " Iteration: " + count);
			Util.logTrace(this, "Tweet language: " + tweet.getText() + "\t" + tweetLanguageGoogle);
		}
		if( tweetNLPFeaturesEnabled ) {
			tweetDates = "";
			tweetLocations = "";
			tweetMentions = "";
			tweetHashtags = "";
			tweetNames = "";
		}
*/
	}

	public void add(Status tweet) {
		try {
			addTweetSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		// Check tweet-ID
		long newID = tweet.getId();

		// Check that it has not been seen before
		if (!seenTweetID.add(new Long(newID))) {
			// This call returns true and adds the element if it is new, but
			// it returns false if the element already existed
			Util.logWarning(this, "Attempt to add a seen tweet, offender=" + tweet.getText());
			addTweetSemaphore.release();
			return;
		}

	//	Util.logInfo(this, "add a seen tweet, newID=" + newID+ " | "+tweet);
		// Check that it is larger than the latest tweetID
		if (newID <= latestID) {
			Util.logWarning(this,
					"Ignored attempt to add a tweet with an ID (" + newID + ") lower than the latest ID added (" + latestID + "), offender=" + tweet.getText());
			addTweetSemaphore.release();
			return;
		}
		latestID = newID;
		
		// Find closest node
		ParentAndDifference res = findClosestNode(tweet);
		Node node = new Node(tweet, res.parent, res.novelty, res.diff, res.isRetweet);
		if (res.parent == null) {
			if( root != null ) {
				throw new IllegalStateException();
			}
			root = node;
		}
		tweets.add(node);

		addTweetSemaphore.release();
	}

	public void addToQueue(SimpleTweet tweet) {
		try {
			addTweetQueueSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}

		long createdAt = tweet.getCreatedAt().getTime();
		urlTweetQueueMap.insertElement(createdAt, tweet);
		addTweetQueueSemaphore.release();
//		Util.logDebug(this,"Added url and tweet to Queue in TwitterTreeAnalyzer, queue size " + urlTweetQueueMap.size()); 
	}

	public void processQueue(String title) {
		/*
		try {
			addTweetQueueSemaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
*/
		
		//Util.logDebug(this, "processQueue() start" + " [urlTweetQueueMap.size: " + urlTweetQueueMap.size() + "]");

//		for (Entry<String, TimeWindowedSortedQueue<SimpleTweet>> entry : urlTweetQueueMap.entrySet()) {
//			TimeWindowedSortedQueue<SimpleTweet> queue = entry.getValue();
			TimeWindowedSortedQueue<SimpleTweet> queue = urlTweetQueueMap;
			if (queue.size() > 0) {
//				String url = entry.getKey();
//				TwitterTreeAnalyzer twitterAnalyzer = new TwitterTreeAnalyzer();
				// String title = TopNURLsPE.getProperty(url, "title");
				long lastTweetId = Long.MIN_VALUE;
				SimpleTweet lastTweet = null;
				Vector<SimpleTweet> oldElements = queue.flushOldElements();
//				addTweetQueueSemaphore.release();
//				Util.logDebug(this,"processQueue() oldElements size: " + oldElements.size());
				for (SimpleTweet tweet: oldElements) {

					// Check tweetIDs are ascending
					if( tweet.getId() < lastTweetId ) {
						Util.logWarning(this, "in output(), newer tweet has lower tweet-id: " + tweet + " vs " + lastTweet );
					}
					lastTweetId = tweet.getId();
					lastTweet = tweet;

					// Add
//					Util.logDebug(this, "processQueue() adding tweet by '" + tweet.getFromUser() + "' on " + tweet.getCreatedAt() );
					if (title != null) {
						add(tweet, title);
				//		Util.logDebug(this,"processQueue() DISABLED add(tweet,title)");
					} else {
						add(tweet);
				//		Util.logDebug(this,"processQueue() DISABLED add(tweet)");
					}
				}
			}
//		}
	}
	/**
	 * 
	 * Note: this is not efficient. Should use a stringbuffer instead.
	 * 
	 * @param inText
	 * @return
	 */
	public static String stripRT(String inText) {
		String text = new String(inText);
		text = text.replaceAll("^ ?RT ?@?[A-Za-z0-9_]+:? ?", " ");
		text = text.replaceAll("^via ?@[A-Za-z0-9_]+:? ?", " ");
		text = text.replaceAll("\"?@[A-Za-z0-9_]+: ", " ");
		text = text.replaceAll(" RT ?@[A-Za-z0-9_]+:? ?", " ");
		text = text.replaceAll(" v[ií]a ?@[A-Za-z0-9_]+:? ?", " ");
		return text;
	}

	/**
	 * 
	 * Note: this is not efficient. Should use a stringbuffer instead.
	 * 
	 * @param inText
	 * @return
	 */

	public static String stripURLs(String inText) {
		Extractor extractor = new Extractor();
		List<String> urls = extractor.extractURLs(inText);
		String text = new String(inText);
		for (String url : urls) {
			text = StringUtils.replace(text, url, "");
		}
		return text;
	}

	/**
	 * Remove "RT @XXXX" and URLs.
	 * 
	 * @param text
	 * @return
	 */
	public static String stripRTandURLs(String text) {
		return stripRT(stripURLs(text)).replaceAll(" +", " ");
	}
	
	private static Pattern rtPattern = Pattern.compile("(\\bRT\\b|\u2672|\u267a|\u267b)\\s@(\\w+)");
	// u2672="UNIVERSAL RECYCLING SYMBOL"
	// u267a="RECYCLING SYMBOL FOR GENERIC MATERIALS"
	// u267b="BLACK UNIVERSAL RECYCLING SYMBOL"
	
	public static String getRTUsername(String text) {
		Matcher matcher = rtPattern.matcher(text);
		if (matcher.find()) {
			return matcher.group(2);
		} else {
			return null;
		}
	}

	/**
	 * The closest node is the one sharing a longest common SUB-SEQUENCE (non-contiguous) with the
	 * current tweet.
	 * 
	 * Note that the difference is computed by removing the longest common SUB-STRING (contiguous).
	 * 
	 * @param tweet
	 * @return
	 */
	ParentAndDifference findClosestNode(Status tweet) {

		String normalizedText = stripRTandURLs(tweet.getText());

		// Empty collection? Parent is null
		if (tweets.size() == 0) {
			return new ParentAndDifference(null, Integer.MAX_VALUE, normalizedText, false);
		}
		
		// Look for "RT " pre-fix
		String userName = getRTUsername(tweet.getText());
		if( userName != null ) {
			for (Node node : tweets ) {
				if( node.getTweet().getUser().getScreenName().equalsIgnoreCase(userName) ) {
					String commonSubstring = Util.longestCommonSubstring(stripRTandURLs(node.getTweet().getText()), normalizedText);
					String diff = StringUtils.replace(normalizedText, commonSubstring, "");
					int distance = normalizedText.length() - commonSubstring.length();
					return new ParentAndDifference(node, distance, diff, true);
				}
			}
		}

		// Try to connect with root if close enough to it
		if (root != null) {
			int distance = normalizedText.length() - Util.longestCommonSubsequence(stripRTandURLs(root.getTweet().getText()), normalizedText).length();
			if (distance <= MAX_DISTANCE_FRACTION_LENGTH_ASSOCIATE_ROOT * normalizedText.length()) {
				String commonSubstring = Util.longestCommonSubstring(stripRTandURLs(root.getTweet().getText()), normalizedText);
				String diff = StringUtils.replace(normalizedText, commonSubstring, "");
				return new ParentAndDifference(root, distance, diff, false);
			}
		}
		
		// Try to connect to another node
		int shortestDistance = Integer.MAX_VALUE;
		Node closestNode = null;
		for (Node node : tweets) {
			String commonSubsequence = Util.longestCommonSubsequence(stripRTandURLs(node.getTweet().getText()), normalizedText);
			int distance = normalizedText.length() - commonSubsequence.length();
			if (distance < shortestDistance) {
				shortestDistance = distance;
				closestNode = node;
			}
		}
		String commonSubstring = Util.longestCommonSubstring(stripRTandURLs(closestNode.getTweet().getText()), normalizedText);
		String diff = StringUtils.replace(normalizedText, commonSubstring, "");
		return new ParentAndDifference(closestNode, shortestDistance, diff, false);
	}

	public JSONObject toJSONObject() {
		try {
			addTweetSemaphore.acquire();
		} catch (InterruptedException ei) {
			ei.printStackTrace();
			throw new IllegalStateException("Interrupted while acquiring semaphore");
		}
		JSONObject obj;
		if (root == null) {
                	Util.logDebug(this,"root in null");
			obj = new JSONObject();
		} else {
			try {
				obj = toJSONObject(root);
			} catch (JSONException e) {
				e.printStackTrace();
				throw new IllegalStateException("Can't create a json object");
			}
		}
		addTweetSemaphore.release();
		return obj;
	}
	
	/**
	 * We use strings when exporting tweet-ids, to avoid overflow when handling the tweet in Javascript.
	 * 
	 * @param tweet
	 * @return
	 */
	private String getIdAsString(Status tweet) {
		return (new Long(tweet.getId())).toString();
	}

	private JSONObject toJSONObject(Node node) throws JSONException {
		Status tweet = node.getTweet();
		JSONObject obj = new JSONObject();

		obj.put("novelty", node.novelty);
		obj.put("diff", node.diff);
               
               
		//Util.logDebug(this,"put count-descendants:"+node.getCountDescendants());
		obj.put("count-descendants", node.getCountDescendants());
		if (opinionScore.containsKey(new Long(tweet.getId()))) {
			obj.put("opinion_score", opinionScore.get(new Long(tweet.getId())));
		}

		obj.put("has_blacklist_term",blacklistFlag);
		obj.put("lang_google",tweetLanguageGoogle);
		obj.put("tweet_dates",tweetDates);
		obj.put("tweet_locations",tweetLocations);
		obj.put("tweet_mentions",tweetMentions);
		obj.put("tweet_hashtags",tweetHashtags);
		obj.put("tweet_names",tweetNames);
		
		obj.put("id", getIdAsString(tweet));
		obj.put("text", tweet.getText());
		
		User user = tweet.getUser();
		obj.put("from_user", user.getScreenName());
		obj.put("user_friends_count", user.getFriendsCount());
		obj.put("user_followers_count", user.getFollowersCount());
		obj.put("user_statuses_count", user.getStatusesCount());
		obj.put("user_profile_url",user.getProfileImageURL());
		
		obj.put("created_at", tweet.getCreatedAt());
		
		JSONArray nearDuplicates = new JSONArray();
		JSONArray retweets = new JSONArray();
		JSONArray variants = new JSONArray();

		for (Node child : node.getChildren()) {
			if (child.novelty < MIN_DISPLAY_NOVELTY_CHARACTERS && ! child.hasChildren()) {
				JSONObject childObj = new JSONObject();
				Status childTweet = child.getTweet();
				childObj.put("id", getIdAsString(childTweet));
				User childUser = childTweet.getUser();
				childObj.put("from_user", childUser.getScreenName());
				childObj.put("user_friends_count", childUser.getFriendsCount());
				childObj.put("user_followers_count", childUser.getFollowersCount());
				childObj.put("user_statuses_count", childUser.getStatusesCount());
				childObj.put("user_profile_url",childUser.getProfileImageURL());
				
				if( child.isRetweet() ) {
					retweets.put(childObj);
				} else {
					nearDuplicates.put(childObj);
				}
			} else {
				if( child.isRetweet() ) {
					retweets.put(toJSONObject(child));
				} else {
					variants.put(toJSONObject(child));
				}
			}
		}

		obj.put("count-near-duplicates", nearDuplicates.length() );
		if( nearDuplicates.length() > 0 ) {
			obj.put("near-duplicates", nearDuplicates);
		}
		obj.put("count-retweets", retweets.length() );
		if( retweets.length() > 0 ) {
			obj.put("retweets", retweets);
		}
		obj.put("count-variants", variants.length() );
		if( variants.length() > 0 ) {
			obj.put("variants", variants);			
		}
		return obj;
	}
	
	public TimeSeries getNoveltySeries() {
		TimeSeries ts = new TimeSeries("twitter-novelty");
		for( Node node: tweets ) {
			ts.insertPoint(new Point( new Long(node.getTweet().getCreatedAt().getTime()), new Double(node.getNovelty())));
		}
		return ts;
	}

	public static void main(String args[]) throws IOException, ClassNotFoundException, JSONException, InterruptedException, ParseException {
		TwitterTreeAnalyzer ta = new TwitterTreeAnalyzer();
		ta.disableTwitterOpinionQuerier();
		
		// Read input file
		File inFile = new File(args[0]);
		List<String> lines = FileUtils.readLines(inFile, "UTF-8");
		for(String line: lines) {
			String[] tokens = line.split("\t", 2);
			SimpleTweet tweet = new SimpleTweet(tokens[1]);
			ta.add( tweet );
		}
		
		// Output
		System.out.println(ta.toJSONObject().toString(4));
	}
}
