package qa.qcri.rtsm.analysis;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;

import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.twitter.TwitterTreeAnalyzer;
import qa.qcri.rtsm.util.Util;

public class TweetOfflineAnalyzer {

	/**
	 * A "novel" tweet must be at least this far away from its most similar tweet.
	 */
	public static int MIN_DISTANCE_DECLARE_NOVEL = 10;

	// changes-Noora
	//final Vector<SimpleTweet> tweets;
	//final Vector<Long> dates;
	static Vector<SimpleTweet> tweets;
	static Vector<Long> dates;
	
	TweetOfflineAnalyzer(File inFile) throws IOException, ParseException, JSONException {
		List<String> lines = FileUtils.readLines(inFile, Util.UTF8.name());

		tweets = new Vector<SimpleTweet>(lines.size());
		dates = new Vector<Long>(lines.size());

		readTweets(lines, inFile.getName());
		/*long lastDate = Long.MIN_VALUE;
		for (String line : lines) {
			String[] tokens = line.split("\t", 2);
			if( tokens.length != 2 ) {
				throw new IllegalArgumentException("Missing token in file '" + inFile.getName() + "': '" + line + "'");
			}
			SimpleTweet tweet = new SimpleTweet(tokens[1]);
			long date = tweet.getCreatedAt().getTime();
			if (date < lastDate) {
				throw new IllegalArgumentException("The tweets are not sorted by increasing date");
			}

			dates.add(new Long(date));
			tweets.add(tweet);
			lastDate = date;
		}*/
	}

	// Noora
	static void readTweets(List<String> lines, String fileName) throws ParseException, JSONException {
		long lastDate = Long.MIN_VALUE;
		for (String line : lines) {
			String[] tokens = line.split("\t", 2);
			if( tokens.length != 2 ) {
				throw new IllegalArgumentException("Missing token in file '" + fileName + "': '" + line + "'");
			}
			SimpleTweet tweet = new SimpleTweet(tokens[1]);
			long date = tweet.getCreatedAt().getTime();
			if (date < lastDate) {
				throw new IllegalArgumentException("The tweets are not sorted by increasing date");
			}

			dates.add(new Long(date));
			tweets.add(tweet);
			lastDate = date;
		}
	}

	// changed to static- Noora
	static double entropy(HashMap<String, Long> frequencies) {
		long numElements = 0;
		for (Long value : frequencies.values()) {
			numElements += value.longValue();
		}

		double entropy = 0.0;
		for (Long value : frequencies.values()) {
			double frequency = (double) (value.longValue()) / (double) numElements;
			entropy -= frequency * (Math.log(frequency) / Math.log(2));
		}

		return entropy;
	}

	//changed to static-Noora
	static TimeSeries computeWordsEntropySeries() {
		HashMap<String, Long> wordFrequencies = new HashMap<String, Long>();
		TimeSeries tsEntropy = new TimeSeries("entropy");
		for (int i = 0; i < tweets.size(); i++) {
			Long date = dates.get(i);
			String tweetText = tweets.get(i).getText();
			String[] tokens = tweetText.split(" ");
			for (String token : tokens) {
				if (wordFrequencies.containsKey(token)) {
					wordFrequencies.put(token, new Long(1 + wordFrequencies.get(token).longValue()));
				} else {
					wordFrequencies.put(token, new Long(1));
				}
			}
			tsEntropy.insertOrReplacePoint(new Point(date, new Double(entropy(wordFrequencies))));
		}
		return tsEntropy;
	}

	//changed to static - Noora
	static TimeSeries computeUniqueTweetsSeries(boolean uniqueFraction) {
		TimeSeries tsUnique = new TimeSeries(uniqueFraction ? "unique-fraction" : "unique");
		int uniqueTweets = 0;

		HashSet<String> seenTexts = new HashSet<String>();

		for (int i = 0; i < tweets.size(); i++) {
			Long date = dates.get(i);
			String text = TwitterTreeAnalyzer.stripRTandURLs(tweets.get(i).getText());

			int minDistance = Integer.MAX_VALUE;
			for (String otherText : seenTexts) {
				String commonSubsequence = Util.longestCommonSubsequence(text, otherText);
				int distance = text.length() - commonSubsequence.length();
				minDistance = distance < minDistance ? distance : minDistance;
			}

			if (minDistance >= MIN_DISTANCE_DECLARE_NOVEL) {
				uniqueTweets++;
			}
			seenTexts.add(text);

			Double value = uniqueFraction ? new Double((double) uniqueTweets / (double) (i + 1)) : new Double(uniqueTweets);
			tsUnique.insertOrReplacePoint(new Point(date, value));
		}
		return tsUnique;
	}

	static boolean isRetweetOf(String text, HashSet<String> accounts) {
		for(String account: accounts) {
			Pattern rtPattern = Pattern.compile("(\\bRT\\b|\u2672|\u267a|\u267b)\\s@" + account, Pattern.CASE_INSENSITIVE);
			if( rtPattern.matcher(text).find() ) {
				return true;
			}
		}
		return false;
	}

	//changed to static - Noora
	static TimeSeries corporateRetweetsFraction(String corporateAccountsStr) {
		HashSet<String> corporateAccounts = new HashSet<String>();
		for( String account: corporateAccountsStr.split(",") ) {
			corporateAccounts.add(account);
		}
		TimeSeries ts = new TimeSeries("corporate-retweets-fraction");
		int seenCorporateRT = 0;
		for (int i = 0; i < tweets.size(); i++) {
			Long date = dates.get(i);
			String text = tweets.get(i).getText();
			if( isRetweetOf(text, corporateAccounts) ) {
				seenCorporateRT ++;
			}
			double value = seenCorporateRT / (double)(i+1);			
			ts.insertOrReplacePoint(new Point(date, new Double(value)));
		}
		return ts;
	}

	public static void usage(Options options, String message) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(ConvertTimeSeries.class.getSimpleName(), "", options, "\n" + message, true);
		System.exit(1);
	}

	public static void usage(Options options) {
		usage(options, "");
	}

	public static void main(String[] args) throws ParseException, IOException, JSONException, org.apache.commons.cli.ParseException {
		Options options = new Options();
		options.addOption("h", "help", false, "Show this help message");
		options.addOption("i", "indir", true, "Directory to read from (created using DataExport --tweets)");
		options.addOption("o", "outdir", true, "Directory to write to");

		options.addOption(null, "word-entropy", false, "Compute word entropy");
		options.addOption(null, "unique", false, "Compute number of unique tweets");
		options.addOption(null, "unique-fraction", false, "Compute fraction of unique tweets");
		options.addOption(null, "corporate-retweets-fraction", true, "Compute fraction of tweets which are corporate-retweets (include comma-separated list of accounts)");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("help")) {
			usage(options);
		}

		if (!cmd.hasOption("indir")) {
			usage(options, "Expected --indir directory");
		}
		if (!cmd.hasOption("outdir")) {
			usage(options, "Expected --outdir directory");
		}

		String inDirectoryName = cmd.getOptionValue("indir");
		File outDirectory = new File(cmd.getOptionValue("outdir"));

		boolean computeEntropy = cmd.hasOption("word-entropy");
		boolean computeUnique = cmd.hasOption("unique");
		boolean computeUniqueFraction = cmd.hasOption("unique-fraction");
		boolean computeCorporateRetweetsFraction = cmd.hasOption("corporate-retweets-fraction");

		File inDir = new File(inDirectoryName);
		File[] listFiles = inDir.listFiles();

		for( int i=0; i<listFiles.length; i++ ) {
			File inFile = listFiles[i];

			TweetOfflineAnalyzer t = new TweetOfflineAnalyzer(inFile);
			Util.logDebug(t, "Processing file " + i + "/" + listFiles.length );

			// Word entropy
			if (computeEntropy) {
				TimeSeries wordEntropy = computeWordsEntropySeries();
				File outFile = new File(outDirectory, inFile.getName() + "-entropy");
				wordEntropy.writeTo(outFile);
			}

			// Unique tweets
			if (computeUnique) {
				TimeSeries uniqueTweets = computeUniqueTweetsSeries(false);
				File outFile = new File(outDirectory, inFile.getName() + "-unique");
				uniqueTweets.writeTo(outFile);
			}

			// Unique fraction of tweets
			if (computeUniqueFraction) {
				TimeSeries uniqueTweets = computeUniqueTweetsSeries(true);
				File outFile = new File(outDirectory, inFile.getName() + "-unique-fraction");
				uniqueTweets.writeTo(outFile);
			}

			// Fraction of tweets which are corporate re-tweets
			if (computeCorporateRetweetsFraction) {
				TimeSeries corporateRetweetsFraction = corporateRetweetsFraction(cmd.getOptionValue("corporate-retweets-fraction"));
				File outFile = new File(outDirectory, inFile.getName() + "-corporate-retweets-fraction");
				corporateRetweetsFraction.writeTo(outFile);
			}
		}
	}
}
