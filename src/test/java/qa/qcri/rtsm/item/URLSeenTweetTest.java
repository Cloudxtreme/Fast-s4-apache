package qa.qcri.rtsm.item;

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Place;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.SymbolEntity;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;

@SuppressWarnings("deprecation")
public class URLSeenTweetTest {
	
	public static URLSeenTweet getURLSeenTweetSample(final String site, final String url, final String fromUser, final String profileImageURL, final String text, final long id) {
		return new URLSeenTweet(site, url, new Status() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public UserMentionEntity[] getUserMentionEntities() {
				return null;
			}
			
			@Override
			public URLEntity[] getURLEntities() {
				return null;
			}
			
			@Override
			public MediaEntity[] getMediaEntities() {
				return null;
			}
			
			@Override
			public HashtagEntity[] getHashtagEntities() {
				return null;
			}
			
			@Override
			public int compareTo(Status o) {
				return 0;
			}
			
			@Override
			public String getText() {
				return text;
			}
			
			@Override
			public String getSource() {
				return null;
			}
				
			@Override
			public Place getPlace() {
				return null;
			}
					
			@Override
			public long getId() {
				return id;
			}
			
			@Override
			public GeoLocation getGeoLocation() {
				return null;
			}
				
			@Override
			public Date getCreatedAt() {
				return new Date();
			}

			@Override
			public long getInReplyToStatusId() {
				return 0;
			}

			@Override
			public int getAccessLevel() {
				return 0;
			}

			@Override
			public RateLimitStatus getRateLimitStatus() {
				return null;
			}

			@Override
			public long[] getContributors() {
				return null;
			}

			@Override
			public long getCurrentUserRetweetId() {
				return 0;
			}

			@Override
			public String getInReplyToScreenName() {
				return null;
			}

			@Override
			public long getInReplyToUserId() {
				return 0;
			}

			@Override
			public int getRetweetCount() {
				return 0;
			}

			@Override
			public Status getRetweetedStatus() {
				return null;
			}

			@Override
			public User getUser() {
				return new User() {

					@Override
					public int compareTo(User o) {
						return 0;
					}

					@Override
					public int getAccessLevel() {
						return 0;
					}

					@Override
					public RateLimitStatus getRateLimitStatus() {
						return null;
					}

					@Override
					public String getBiggerProfileImageURL() {
						return null;
					}

					@Override
					public String getBiggerProfileImageURLHttps() {
						return null;
					}

					@Override
					public Date getCreatedAt() {
						return null;
					}

					@Override
					public String getDescription() {
						return null;
					}

					@Override
					public int getFavouritesCount() {
						return 0;
					}

					@Override
					public int getFollowersCount() {
						return 0;
					}

					@Override
					public int getFriendsCount() {
						return 0;
					}

					@Override
					public long getId() {
						return 0;
					}

					@Override
					public String getLang() {
						return null;
					}

					@Override
					public int getListedCount() {
						return 0;
					}

					@Override
					public String getLocation() {
						return null;
					}

					@Override
					public String getMiniProfileImageURL() {
						return null;
					}

					@Override
					public String getMiniProfileImageURLHttps() {
						return null;
					}

					@Override
					public String getName() {
						return null;
					}

					@Override
					public String getOriginalProfileImageURL() {
						return null;
					}

					@Override
					public String getOriginalProfileImageURLHttps() {
						return null;
					}

					@Override
					public String getProfileBackgroundColor() {
						return null;
					}

					@Override
					public String getProfileBackgroundImageURL() {
						return null;
					}

					@Override
					public String getProfileBackgroundImageUrl() {
						return null;
					}

					@Override
					public String getProfileBackgroundImageUrlHttps() {
						return null;
					}

					@Override
					public String getProfileBannerIPadRetinaURL() {
						return null;
					}

					@Override
					public String getProfileBannerIPadURL() {
						return null;
					}

					@Override
					public String getProfileBannerMobileRetinaURL() {
						return null;
					}

					@Override
					public String getProfileBannerMobileURL() {
						return null;
					}

					@Override
					public String getProfileBannerRetinaURL() {
						return null;
					}

					@Override
					public String getProfileBannerURL() {
						return null;
					}

					@Override
					public String getProfileImageURL() {
						return profileImageURL;
					}

					@Override
					public String getProfileImageURLHttps() {
						return null;
					}

					@Override
					public URL getProfileImageUrlHttps() {
						return null;
					}

					@Override
					public String getProfileLinkColor() {
						return null;
					}

					@Override
					public String getProfileSidebarBorderColor() {
						return null;
					}

					@Override
					public String getProfileSidebarFillColor() {
						return null;
					}

					@Override
					public String getProfileTextColor() {
						return null;
					}

					@Override
					public String getScreenName() {
						return fromUser;
					}

					@Override
					public Status getStatus() {
						return null;
					}

					@Override
					public int getStatusesCount() {
						return 0;
					}

					@Override
					public String getTimeZone() {
						return null;
					}

					@Override
					public String getURL() {
						return null;
					}

					@Override
					public int getUtcOffset() {
						return 0;
					}

					@Override
					public boolean isContributorsEnabled() {
						return false;
					}

					@Override
					public boolean isFollowRequestSent() {
						return false;
					}

					@Override
					public boolean isGeoEnabled() {
						return false;
					}

					@Override
					public boolean isProfileBackgroundTiled() {
						return false;
					}

					@Override
					public boolean isProfileUseBackgroundImage() {
						return false;
					}

					@Override
					public boolean isProtected() {
						return false;
					}

					@Override
					public boolean isShowAllInlineMedia() {
						return false;
					}

					@Override
					public boolean isTranslator() {
						return false;
					}

					@Override
					public boolean isVerified() {
						return false;
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
					}
				};
			}

			@Override
			public boolean isFavorited() {
				return false;
			}

			@Override
			public boolean isPossiblySensitive() {
				return false;
			}

			@Override
			public boolean isRetweet() {
				return false;
			}

			@Override
			public boolean isRetweetedByMe() {
				return false;
			}

			@Override
			public boolean isTruncated() {
				return false;
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

		} );
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testURLSeenTweetStringTweet() {
		URLSeenTweet urlSeenTweet = getURLSeenTweetSample("www.example2.com", "http://www.example2.com/path/to/url.html?xxx", "abc", "abcxyz", "xyz", 0);
		assertEquals( "www.example2.com", urlSeenTweet.getSite());
		assertEquals( "xyz", urlSeenTweet.getSimpleTweet().getText());
	}
}
