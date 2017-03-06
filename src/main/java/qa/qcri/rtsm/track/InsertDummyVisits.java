package qa.qcri.rtsm.track;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.math.RandomUtils;

import qa.qcri.rtsm.util.Util;
import qa.qcri.rtsm.util.WebUtil;

public class InsertDummyVisits {

	private static final String[] SOURCES = { "(none)", "google", "yahoo", "microsoft", "facebook" };

	private static final String[] SEARCH_TERMS = { "(none)", "search one", "search two", "search three" };

	static class Visitor implements Runnable {

		private static final String TRACKING_ENDPOINT = "http://localhost:8080/";

		// private static final String TRACKING_ENDPOINT = "http://track.qcri.org/log.php";

		/**
		 * Maximum time to wait between requests, in seconds.
		 */
		private static final int DEFAULT_WAIT = 0;

		final String siteID;
		
		final String url;

		final int count;
		
		final int wait;

		public Visitor(String siteID, String url, int maxVisits, boolean randomize, int wait) {
			this.siteID = siteID;
			this.url = url;
			
			if( randomize ) {
				this.count = (int) Math.ceil(Math.random() * maxVisits);
			} else {
				this.count = maxVisits;
			}
			this.wait = wait;
			System.err.println("Will insert " + this.count + " visits to url " + this.url + " waiting up to " + this.wait + " seconds between requests");
		}

		@Override
		public void run() {
			for (int i = 0; i < count; i++) {

				StringBuffer trackURL = new StringBuffer();
				trackURL.append(TRACKING_ENDPOINT);

				try {
					// Add url
					trackURL.append("?");
					trackURL.append(VisitEventListener.KEY_URL);
					trackURL.append("=");
					trackURL.append(URLEncoder.encode(url, "UTF-8"));
					
					// Add siteID
					trackURL.append("&");
					trackURL.append(VisitEventListener.KEY_SITE_ID);
					trackURL.append("=");
					trackURL.append(siteID);

					// Add visitorID
					trackURL.append("&");
					trackURL.append(VisitEventListener.KEY_VISITOR_ID);
					trackURL.append("=");
					trackURL.append("user" + RandomUtils.nextLong());
					
					// Add source
					String source = randomSource();
					trackURL.append("&");
					trackURL.append(VisitEventListener.KEY_SOURCE);
					trackURL.append("=");
					trackURL.append(URLEncoder.encode(source, "UTF-8"));

					// Add search terms
					String searchTerms = source.equals("(none)") ? "(none)" : randomSearchTerms();
					trackURL.append("&");
					trackURL.append(VisitEventListener.KEY_SEARCH_TERMS);
					trackURL.append("=");
					trackURL.append(URLEncoder.encode(searchTerms, "UTF-8"));
					
					// Add sample rate
					trackURL.append("&");
					trackURL.append(VisitEventListener.KEY_SAMPLE_RATE);
					trackURL.append("=");
					trackURL.append(URLEncoder.encode( VisitEventListener.SUBKEY_SAMPLE_RATE + "=" + "1", "UTF-8") );

				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}

				Util.logDebug(this, trackURL.toString());
				String result = WebUtil.getHTMLContentsAsString(trackURL.toString());
				if( result == null ) {
					throw new IllegalStateException("URL " + trackURL.toString() + " can not be reached");
				}
				System.err.println(trackURL.toString());

				// Wait
				if( wait > 0 ) {
					long rndWait = 1000 * ((long) Math.ceil(Math.random() * (double)wait));
					try {
						Thread.sleep(rndWait);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public static String randomSource() {
		return SOURCES[RandomUtils.nextInt(SOURCES.length)];
	}

	public static String randomSearchTerms() {
		return SEARCH_TERMS[RandomUtils.nextInt(SEARCH_TERMS.length)];
	}
	
	public static void usage(Options options, String message) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(InsertDummyVisits.class.getSimpleName(), "", options, "\n" + message, true);
		System.exit(1);
	}
	
	public static void usage(Options options) {
		usage(options, "");
	}
	
	public static void main(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption("h", "help", false, "Show this help message");
		
		options.addOption("s", "siteid", true, "The site-id to include in the requests");
		options.addOption("r", "randomize", false, "Randomize the number of visits (up to 'nvisits')");
		options.addOption("n", "nvisits", true, "Number of visits");
		options.addOption("w", "wait", true, "Max. seconds to wait between request (will be randomized), default 0");
	
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		
		if( cmd.hasOption("help") ) {
			usage(options);
		}
		
		String siteID = cmd.getOptionValue("siteid");
		int nvisits = Integer.parseInt(cmd.getOptionValue("nvisits"));
		boolean randomize = cmd.hasOption("randomize");
		int wait = cmd.hasOption("wait") ? Integer.parseInt(cmd.getOptionValue("wait")) : Visitor.DEFAULT_WAIT;
		
		if( cmd.getArgs().length == 0 ) {
			usage(options, "Must specify at least one URL to visit");
		}
				
		for (String url : cmd.getArgs()) {
			(new Thread(new Visitor(siteID, url, nvisits, randomize, wait))).start();
		}
	}
}
