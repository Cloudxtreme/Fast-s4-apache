package qa.qcri.rtsm.util;

public class RTSMConf {
	// Tip: use via tunnel by doing:
	// ssh -f -L 9160:localhost:9160 sc1.qcri.org -N
	public static String CASSANDRA_HOST = "localhost";
	
	public static int CASSANDRA_PORT = 9160;
	
	public static String TWITTER_OPINION_HOST = "localhost";
	
	public static int TWITTER_OPINION_PORT = 1456;

	/**
	 * Remove this before releasing the application.
	 * Access tokens for the application are configured in rtsm-visit-conf.xml
	 * 
	 * Get your own by creating an app on Facebook.
	 * - Go to developers.facebook.com
	 * - Tools > App Dashboard
	 * - Select the app you created, then look at its "appid" (e.g. APPID=411989452188096)
	 * - Click on "edit settings" of your app, and look under website the "site URL" (e.g. SITE=http://qcri.qa/)
	 * - Replace APPID and SITE in the URL below:
	 * https://www.facebook.com/dialog/permissions.request?app_id=APPID&display=popup&next=SITE&type=user_agent&perms=offline_access&fbconnect=1
	 * - You will be redirected to SITE#access_token=ACCESSTOKEN&expires_in=...&code=...
	 * - Copy the ACCESSTOKEN
	 * - You can debug that it is correct here: https://developers.facebook.com/tools/debug/access_token?q=ACCESSTOKEN 
	 * 
	 * Note that access tokens expire every 2 months.
	 * 
	 */
	// expires Oct '13
	public static final String TEST_FACEBOOK_ACCESS_TOKEN = "CAAF2s8Ty6cABAIBkFi3GLgoPkvDZAO2Q5nOsOH4rlQINsR0fii8378JFKjxZCAa9OK4ZB4PrcgnxH2bDzb5SHV9fpOuV3PZC7KL1Onk0EXFVvO5IYupsBGiMzW3RjDnD18wT2r0AB4s9w9wdQxs4oLa2Ui8ZBcieydOwJwuzr96zkvbtcx9G2";
	// "AAAF2s8Ty6cABAMUIHDib9O8hsQGYLaQzzQPRCiFROZA2EfE3I3llXlaKt49SbbOCzQMiCsjA7COY9VFgurNZABtm2pALHu4NQ6VkQ8bAZDZD";	
	
	/**
	 * Remove this before releasing the application.
	 * Access tokens for the application are configured in rtsm-visit-conf.xml
	 * 
	 * This is the username of the person that requests the access token above.
	 */
	public static final String TEST_FACEBOOK_ACCESS_TOKEN_USER = "gvrkiran";

	/**
	 * Remove this before releasing the application.
	 * 
	 * GEt your own by creating an app on Twitter, then generating an access token.
	 */
	public static String TEST_TWITTER_CONSUMER_KEY = "cd2CbYFSSkRi20hQmTaaQ";

	public static String TEST_TWITTER_CONSUMER_SECRET = "zdRugnKMqBmiIRVLbZrm2DEouQS3w5YpR0naYyHSYCY";

	// ChaTo
	/*
	public static String TEST_TWITTER_OAUTH_ACCESS_TOKEN = "5734242-T7YwJSZ3PzaMUhf4XeDBboduYzOFikHDJ9zXVXR0g";
	public static String TEST_TWITTER_OAUTH_ACCESS_TOKEN_SECRET = "VZr1beowvLkZ7amhVzkEXx1z68oks4hm8JCUGeRDw";
	*/
	
	// Janette
	public static String TEST_TWITTER_OAUTH_ACCESS_TOKEN = "82573938-P7Jxtaa9az1wMQH92Bi9nyGfWBiRrlMveAzAYsmqC";
	public static String TEST_TWITTER_OAUTH_ACCESS_TOKEN_SECRET = "Yt4ovneNSiONWnFLU8gU898Zvx5QNDd94UmOdNE8k";
	
	public static String LANGUAGE_PROFILES_DIR = "/opt/rtsm-lang-profiles";
	
	public RTSMConf() {
		
	}
	
	public String getAppName() {
		return "RTSM";
	}
}
