package qa.qcri.rtsm.twitter;

import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.NotImplementedException;
import org.json.JSONException;
import org.json.JSONObject;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.SymbolEntity;
import twitter4j.Place;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

public class SimpleTweet implements Status {

	private static final String KEY_FROM_USER = "fromUser";
	private static final String KEY_PROFILE_IMAGE_URL = "profileImageURL";
	private static final String KEY_USER_FRIENDS_COUNT = "userFriendsCount";
	private static final String KEY_USER_FOLLOWERS_COUNT = "userFollowersCount";
	private static final String KEY_USER_STATUSES_COUNT = "userStatusesCount";
	
	private static final String KEY_CREATED_AT = "createdAt";
	private static final String KEY_TEXT = "text";
	private static final String KEY_ID = "id";
	private static final String KEY_USER_LOCATION = "userLocation";
	private static final String KEY_GEO_LOCATION_STR = "geoLocationStr";

	private static final long serialVersionUID = 1L;
	
	protected String fromUser;
	protected String profileImageURL;
	protected int userFriendsCount;
	protected int userFollowersCount;
	protected int userStatusesCount;
	protected long createdAt;
	protected String text;
	protected long id;
	protected String userLocation;
	protected String geoLocationStr;
	
	public SimpleTweet() {
		
	}
	
	public SimpleTweet(Status tweet) {
		User user = tweet.getUser();
		this.fromUser = new String(user.getScreenName());
		this.profileImageURL = new String(user.getProfileImageURL());
		this.userFriendsCount = user.getFriendsCount();
		this.userFollowersCount = user.getFollowersCount();
		this.userStatusesCount = user.getStatusesCount();
		
		this.createdAt = tweet.getCreatedAt().getTime();
		this.text = new String(tweet.getText());
		this.id = tweet.getId();
		
		this.userLocation = user.getLocation() == null ? new String("") : new String(user.getLocation());
		this.geoLocationStr = tweet.getGeoLocation() == null ? new String("") : new String(tweet.getGeoLocation().toString());
	}
	
	public SimpleTweet(SimpleTweet other) {
		throw new NotImplementedException();
	}
	
	public SimpleTweet(String value) throws ParseException, JSONException {
		JSONObject json = new JSONObject(value);
		this.fromUser = json.getString(KEY_FROM_USER);
		this.profileImageURL = json.getString(KEY_PROFILE_IMAGE_URL);
		this.userFriendsCount = Integer.parseInt(json.getString(KEY_USER_FRIENDS_COUNT));
		this.userFollowersCount = Integer.parseInt(json.getString(KEY_USER_FOLLOWERS_COUNT));
		this.userStatusesCount = Integer.parseInt(json.getString(KEY_USER_STATUSES_COUNT));
		
		this.createdAt = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).parse(json.getString(KEY_CREATED_AT)).getTime();
		this.text = json.getString(KEY_TEXT);
		this.id = json.getLong(KEY_ID);
		
		// For backwards-compatibility, test that these existed
		this.userLocation = json.has(KEY_USER_LOCATION) ? json.getString(KEY_USER_LOCATION) : "";  
		this.geoLocationStr = json.has(KEY_GEO_LOCATION_STR) ? json.getString(KEY_GEO_LOCATION_STR) : "";
	}

	public JSONObject toJSON() {
		JSONObject json = new JSONObject();
		try {
			json.put( KEY_FROM_USER, fromUser );
			json.put( KEY_PROFILE_IMAGE_URL, profileImageURL );
			json.put( KEY_USER_FRIENDS_COUNT, new Integer(userFriendsCount));
			json.put( KEY_USER_FOLLOWERS_COUNT, new Integer(userFollowersCount));
			json.put( KEY_USER_STATUSES_COUNT, new Integer(userStatusesCount));
			json.put( KEY_CREATED_AT, DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).format(new Date(createdAt)) );
			json.put( KEY_TEXT, text );
			json.put( KEY_ID, new Long(id)); 
			json.put( KEY_USER_LOCATION, userLocation );
			json.put( KEY_GEO_LOCATION_STR, geoLocationStr );
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return json;
	}
	
	public String getFromUser() {
		return fromUser;
	}

	public void setFromUser(String fromUser) {
		this.fromUser = fromUser;
	}
	
	public String getProfileImageURL() {
		return profileImageURL;
	}

	public void setProfileImageURL(String profileImageURL) {
		this.profileImageURL = profileImageURL;
	}
	
	public long getUserFriendsCount() {
		return userFriendsCount;
	}

	public void setUserFriendsCount(int userFriendsCount) {
		this.userFriendsCount = userFriendsCount;
	}

	public long getUserFollowersCount() {
		return userFollowersCount;
	}

	public void setUserFollowersCount(int userFollowersCount) {
		this.userFollowersCount = userFollowersCount;
	}

	public long getUserStatusesCount() {
		return userStatusesCount;
	}

	public void setUserStatusesCount(int userStatusesCount) {
		this.userStatusesCount = userStatusesCount;
	}

	public Date getCreatedAt() {
		return new Date(createdAt);
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt.getTime();
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public String getUserLocation() {
		return userLocation;
	}

	public void setUserLocation(String userLocation) {
		this.userLocation = userLocation;
	}
	
	public String getGeoLocationStr() {
		return geoLocationStr;
	}

	public void setGeoLocationStr(String geoLocationStr) {
		this.geoLocationStr = geoLocationStr;
	}
	
	@Override
	public int compareTo(Status o) {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public HashtagEntity[] getHashtagEntities() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public MediaEntity[] getMediaEntities() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public URLEntity[] getURLEntities() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public UserMentionEntity[] getUserMentionEntities() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public GeoLocation getGeoLocation() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public long getId() {
		return id;
	}
	
	public void setId(long id) {
		this.id = id;
	}

	@Override
	public Place getPlace() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public String getSource() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public long getInReplyToStatusId() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public int getAccessLevel() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public RateLimitStatus getRateLimitStatus() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public long[] getContributors() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public long getCurrentUserRetweetId() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public String getInReplyToScreenName() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public long getInReplyToUserId() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public int getRetweetCount() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public Status getRetweetedStatus() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public User getUser() {
		return new User() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public int compareTo(User o) {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public int getAccessLevel() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public RateLimitStatus getRateLimitStatus() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getBiggerProfileImageURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getBiggerProfileImageURLHttps() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public Date getCreatedAt() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getDescription() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public int getFavouritesCount() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public int getFollowersCount() {
				return userFollowersCount;
			}

			@Override
			public int getFriendsCount() {
				return userFriendsCount;
			}

			@Override
			public long getId() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getLang() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public int getListedCount() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getLocation() {
				return userLocation;
			}

			@Override
			public String getMiniProfileImageURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getMiniProfileImageURLHttps() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getName() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getOriginalProfileImageURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getOriginalProfileImageURLHttps() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBackgroundColor() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBackgroundImageURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBackgroundImageUrl() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBackgroundImageUrlHttps() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBannerIPadRetinaURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBannerIPadURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBannerMobileRetinaURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBannerMobileURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBannerRetinaURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileBannerURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileImageURL() {
				return profileImageURL;
				// throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileImageURLHttps() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public URL getProfileImageUrlHttps() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileLinkColor() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileSidebarBorderColor() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileSidebarFillColor() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getProfileTextColor() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getScreenName() {
				return fromUser;
			}

			@Override
			public Status getStatus() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public int getStatusesCount() {
				return userStatusesCount;
			}

			@Override
			public String getTimeZone() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public String getURL() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public int getUtcOffset() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isContributorsEnabled() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isFollowRequestSent() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isGeoEnabled() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isProfileBackgroundTiled() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isProfileUseBackgroundImage() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isProtected() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isShowAllInlineMedia() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isTranslator() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public boolean isVerified() {
				throw new IllegalStateException("Not implemented");
			}

			@Override
			public URLEntity[] getDescriptionURLEntities() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public URLEntity getURLEntity() {
				// TODO Auto-generated method stub
				return null;
			}};
	}

	@Override
	public boolean isFavorited() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public boolean isPossiblySensitive() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public boolean isRetweet() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public boolean isRetweetedByMe() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public boolean isTruncated() {
		throw new IllegalStateException("Not implemented");
	}

	@Override
	public String toString() {
		return toJSON().toString();
	}
	@Override
	public SymbolEntity[] getSymbolEntities() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getFavoriteCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getIsoLanguageCode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRetweeted() {
		// TODO Auto-generated method stub
		return false;
	}
}
