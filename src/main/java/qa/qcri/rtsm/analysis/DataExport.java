package qa.qcri.rtsm.analysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import qa.qcri.rtsm.analysis.TimeSeries.EmptySeriesException;
import qa.qcri.rtsm.analysis.imran.FacebookShareAnalysis;
import qa.qcri.rtsm.analysis.imran.TimeSeriesIntervals;
import qa.qcri.rtsm.analysis.imran.TweetsAnalysis;
import qa.qcri.rtsm.analysis.imran.VisitsAnalysis;
import qa.qcri.rtsm.persist.cassandra.CassandraPersistentTimeSeries;
import qa.qcri.rtsm.persist.cassandra.CassandraPersistentTweet;
import qa.qcri.rtsm.persist.cassandra.CassandraSchema;
import qa.qcri.rtsm.process.TimeSeriesSourcePreparePersistPE;
import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.util.Util;

public abstract class DataExport {

	static final int WINDOW_SIZE_IN_MINUTES = 60;
	
	static final int WINDOW_SIZE_IN_SECONDS = WINDOW_SIZE_IN_MINUTES*60;
	
	static final int WINDOW_SIZE_IN_MILLIS = WINDOW_SIZE_IN_SECONDS * 1000;

	static final String KEY_ONE_MINUTE = TimeSeriesIntervals.KEY_ONE_MINUTE;
	
	static final int ONE_MINUTE_IN_SECONDS = 60;
	
	static final int ONE_MINUTE_IN_MILLIS = ONE_MINUTE_IN_SECONDS * 1000;

	final String outDirectory;
	
	/**
	 * Set output directory parameter and ensure the directory exists.
	 * 
	 * @param outDirectory
	 */
	private DataExport(String outDirectory) {
		this.outDirectory = outDirectory;
		
		File dir = new File(outDirectory);
		if( ! dir.exists() ) {
			dir.mkdir();
			if( ! dir.exists() ) {
				throw new IllegalArgumentException("Directory does not exist and could not be created: '" + outDirectory + "'");
			}
		}
	}

	static class DataExportFacebook extends DataExport {

		private FacebookShareAnalysis fbAnalysis;

		DataExportFacebook(String outDirectory) {
			super(outDirectory);
			fbAnalysis = new FacebookShareAnalysis();
			fbAnalysis.getTimeSeriesKeyspace();
		}

		@Override
		public void exportURL(String url) throws FileNotFoundException {
			TimeSeries fbTS;
			try {
				fbTS = fbAnalysis.getTimeSeries(url, TimeSeriesIntervals.KEY_ONE_MINUTE_FB, new Integer(0));
			} catch (EmptySeriesException e) {
				Util.logDebug( this, "No facebook shares for URL " + url );
				return;
			}
			TimeSeries data = fbTS.relativeSeriesIgnoreDescending();
			TimeSeries movingAverage = data.movingAverage(WINDOW_SIZE_IN_MILLIS, ONE_MINUTE_IN_MILLIS);
			write(getFilename(url), new TimeSeries[] { data, movingAverage } );
		}

		@Override
		String getFilename(String url) {
			return "facebook-" + encode(url) + ".csv";
		}
	}

	static class DataExportTwitter extends DataExport {

		private TweetsAnalysis twitterAnalysis;

		DataExportTwitter(String outDirectory) {
			super(outDirectory);
			twitterAnalysis = new TweetsAnalysis();
			twitterAnalysis.getTweetsKeyspace();
		}

		@Override
		public void exportURL(String url) throws FileNotFoundException {
			TimeSeries ts;
			try {
				ts = twitterAnalysis.getTimeSeries(url, ONE_MINUTE_IN_SECONDS );
			} catch( EmptySeriesException e ) {
				Util.logDebug( this, "No tweets for URL " + url );
				return;
			}
			TimeSeries movingAverage = ts.movingAverage(WINDOW_SIZE_IN_MILLIS, ONE_MINUTE_IN_MILLIS);
			write(getFilename(url), new TimeSeries[] { ts, movingAverage } );
		}

		@Override
		String getFilename(String url) {
			return "twitter-" + encode(url) + ".csv";
		}
	}
	
	static class DataExportTweets extends DataExport {

		private CassandraPersistentTweet persistentTweets;

		DataExportTweets(String outDirectory) {
			super(outDirectory);
			persistentTweets = new CassandraPersistentTweet();
			persistentTweets.setColumnFamilyName(CassandraSchema.COLUMNFAMILY_NAME_TWEETS);
		}

		@Override
		public void exportURL(String url) throws FileNotFoundException {
			TreeMap<Long, SimpleTweet> tweets = persistentTweets.get(url);
			if( tweets.size() > 0 ) {
				write(getFilename(url), tweets);
			} else {
				Util.logDebug( this, "No tweets for URL " + url );
				return;
			}
		}

		private void write(String filename, TreeMap<Long, SimpleTweet> tweets) throws FileNotFoundException {
			PrintWriter pw = getPrintWriter(filename);
			for (Long tweetID : tweets.keySet()) {
				SimpleTweet tweet = tweets.get(tweetID);
				Long date = new Long(tweet.getCreatedAt().getTime());
				pw.print(date);
				pw.print("\t");
				pw.print(tweet.toJSON().toString());
				pw.print("\n");
			}
			pw.close();
		}

		@Override
		String getFilename(String url) {
			return "tweets-" + encode(url) + ".csv";
		}
	}

	static class DataExportVisits extends DataExport {

		private CassandraPersistentTimeSeries persistentVisits;

		DataExportVisits(String outDirectory) {
			super(outDirectory);
			persistentVisits = new CassandraPersistentTimeSeries();
			persistentVisits.setColumnFamilyName(CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_VISITS);
		}

		@Override
		public void exportURL(String url) throws FileNotFoundException {
			TimeSeries ts = persistentVisits.getTimeSeries(url, KEY_ONE_MINUTE);
			TimeSeries movingAverage = ts.movingAverage(WINDOW_SIZE_IN_MILLIS, ONE_MINUTE_IN_MILLIS);

			write(getFilename(url), new TimeSeries[] { ts, movingAverage });
		}

		@Override
		String getFilename(String url) {
			return "visits-" + encode(url) + ".csv";
		}
	}
	
	static class DataExportSource extends DataExport {

		private CassandraPersistentTimeSeries persistentTimeSeries;

		DataExportSource(String outDirectory) {
			super(outDirectory);
			persistentTimeSeries = new CassandraPersistentTimeSeries();
			persistentTimeSeries.setColumnFamilyName(CassandraSchema.COLUMNFAMILY_NAME_TIMESERIES_SOURCES);
		}

		@Override
		public void exportURL(String url) throws FileNotFoundException {
			TimeSeries organic = persistentTimeSeries.getTimeSeries(url, TimeSeriesSourcePreparePersistPE.KEY_ORGANIC_ONE_MINUTE);
			TimeSeries direct = persistentTimeSeries.getTimeSeries(url, TimeSeriesSourcePreparePersistPE.KEY_DIRECT_ONE_MINUTE);
			TimeSeries internal = persistentTimeSeries.getTimeSeries(url, TimeSeriesSourcePreparePersistPE.KEY_INTERNAL_ONE_MINUTE);
			TimeSeries referral = persistentTimeSeries.getTimeSeries(url, TimeSeriesSourcePreparePersistPE.KEY_REFERRAL_ONE_MINUTE);
			
			if( organic.size() + direct.size() + internal.size() + referral.size() > 0 ) {
				write(getFilename(url), new TimeSeries[] { organic, direct, internal, referral } );
			} else {
				Util.logDebug(this, "Nothing for " + url + ":" + TimeSeriesSourcePreparePersistPE.KEY_ORGANIC_ONE_MINUTE);
			}
		}

		@Override
		String getFilename(String url) {
			// ODIR stands for organic-direct-internal-referral
			return "source-o_d_i_r-" + encode(url) + ".csv";
		}
	}
	
	static class DataExportList extends DataExport {
		
		private VisitsAnalysis visitAnalysis;

		DataExportList(String outDirectory) {
			super(outDirectory);
			visitAnalysis = new VisitsAnalysis();
			visitAnalysis.getTimeSeriesKeyspace();
		}
		
		public List<String> getURLsByPrefix(String prefix) {
			return visitAnalysis.getTimeSeriesKeyspace().getAllArticles(prefix);
		}
		
		@Override
		public void exportByPrefix(String prefix) {
			List<String> keysList = getURLsByPrefix(prefix);
			try {
				write(getFilename(prefix), keysList);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		@Override
		String getFilename(String prefix) {
			return "list-" + encode(prefix) + ".csv";
		}

		@Override
		void exportURL(String url) throws FileNotFoundException {
			throw new IllegalArgumentException("This can not export a single URL");
		}
	}

	PrintWriter getPrintWriter(String fileName) throws FileNotFoundException {
		return new PrintWriter(new FileOutputStream(new File(getOutDirectory(), fileName)));
	}

	void write(String fileName, SortedMap<Long, Integer> data) throws FileNotFoundException {
		PrintWriter pw = getPrintWriter(fileName);
		for (Long key : data.keySet()) {
			int value = data.get(key).intValue();
			pw.print(key);
			pw.print("\t");
			pw.print(value);
			pw.print("\n");
		}
		pw.close();
	}
	
	void write(String fileName, List<String> keysList) throws FileNotFoundException {
		PrintWriter pw = getPrintWriter(fileName);
		for( String key: keysList ) {
			pw.print(key);
			pw.print("\n");
		}
		pw.close();
	}

	void write(String fileName, SortedMap<Long, Integer> data1, SortedMap<Long, Double> data2) throws FileNotFoundException {
		PrintWriter pw = getPrintWriter(fileName);
		for (Long key : data1.keySet()) {
			int value1 = data1.get(key).intValue();
			double value2 = data2.get(key).doubleValue();
			pw.print(key);
			pw.print("\t");
			pw.print(value1);
			pw.print("\t");
			pw.print(value2);
			pw.print("\n");
		}
		pw.close();
	}
	
	void write(String filename, TimeSeries[] data) throws FileNotFoundException {
		PrintWriter pw = getPrintWriter(filename);
		TreeSet<Long> allDates = new TreeSet<Long>();
		for( TimeSeries ts: data ) {
			allDates.addAll(ts.getDates());
		}
		
		for (Long key : allDates ) {
			pw.print(key);
			for( TimeSeries ts: data ) {
				pw.print( "\t" );
				pw.print( ts.get(key) );
			}
			pw.print("\n");
		}
		pw.close();	
	}

	public String getOutDirectory() {
		return outDirectory;
	}

	public String encode(String url) {
		return url.replaceAll("[/:]", "_");
	}

	abstract String getFilename(String url);

	abstract void exportURL(String url) throws FileNotFoundException;
	
	public void exportByPrefix(String prefix) {
		DataExportList dataList = new DataExportList(outDirectory);
		List<String> urlList = dataList.getURLsByPrefix(prefix);
		if( urlList.size() == 0 ) {
			Util.logWarning(this, "There are no URLs with visits");
		}
		for( int i=0; i<urlList.size(); i++ ) {
			try {
				exportURL(urlList.get(i));
				Util.logDebug(this, "Exporting url " + i + "/" + urlList.size() );
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				Util.logDebug(this, "Skipped url " + i + "/" + urlList.size() + " " + urlList.get(i) );
			}
		}
	}
	
	public static void usage(Options options, String message) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(DataExport.class.getSimpleName(), "", options, "\n" + message, true);
		System.exit(1);
	}
	
	public static void usage(Options options) {
		usage(options, "");
	}

	public static void main(String[] args) throws ParseException, FileNotFoundException {
		Options options = new Options();
		options.addOption("h", "help", false, "Show this help message");
		options.addOption("o", "outdir", true, "Directory to write to");
		
		OptionGroup whatToShow = new OptionGroup();
		whatToShow.addOption( new Option("l", "list", false, "Export list of URLs that have visits") );
		whatToShow.addOption( new Option("v", "visits", false, "Export number of visits per 1-minute window (and moving average)") );
		whatToShow.addOption( new Option("f", "facebook", false, "Export number of facebook per 10-minute window (and moving average)") );
		whatToShow.addOption( new Option("t", "twitter", false, "Export number of tweets per 10-minute window (and moving average)") );
		whatToShow.addOption( new Option("s", "source", false, "Export the source statistics per 1-minute window (and moving average)"));
		whatToShow.addOption( new Option(null, "tweets", false, "Export the tweets") );
		options.addOptionGroup(whatToShow);
		
		OptionGroup urlOrPrefix = new OptionGroup();
		urlOrPrefix.addOption( new Option("u", "url", true, "A single URL to be retrieved") );
		urlOrPrefix.addOption( new Option("p", "url-prefix", true, "An entire prefix of URLs to be retrieved (e.g. 'http://www.aljazeera.com/'") );
		options.addOptionGroup(urlOrPrefix);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		
		if( cmd.hasOption("help") ) {
			usage(options);
		}
		
		DataExport dataExport = null;

		String outDirectory = null;

		if (cmd.hasOption("outdir")) {
			outDirectory = cmd.getOptionValue("outdir");
		} else {
			usage(options, "Must supply an output directory with --outdir");
		}

		if (cmd.hasOption("visits")) {
			dataExport = new DataExportVisits(outDirectory);
		} else if (cmd.hasOption("source") ) {
			dataExport = new DataExportSource(outDirectory);
		} else if (cmd.hasOption("facebook")) {
			dataExport = new DataExportFacebook(outDirectory);
		} else if (cmd.hasOption("twitter")) {
			dataExport = new DataExportTwitter(outDirectory);
		} else if (cmd.hasOption("tweets")) {
			dataExport = new DataExportTweets(outDirectory);
		} else if (cmd.hasOption("list")) {
			dataExport = new DataExportList(outDirectory);
		} else {
			usage(options, "Must indicate what to extract: --visits, --source, --facebook, --twitter, --tweets or --list");
		}

		if (cmd.hasOption("url")) {
			dataExport.exportURL(cmd.getOptionValue("url"));
		} else if( cmd.hasOption("url-prefix") ) {
			dataExport.exportByPrefix(cmd.getOptionValue("url-prefix") );
		} else {
			usage(options, "Must specify --url or --url-prefix");
		}
	}
}
