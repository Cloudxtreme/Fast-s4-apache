package qa.qcri.rtsm.analysis;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.json.JSONException;

import qa.qcri.rtsm.twitter.SimpleTweet;
import qa.qcri.rtsm.util.Util;

public class TweetUsersOfflineAnalyzer {

	final Vector<SimpleTweet> tweets;

	long[] dates;

	double[] statuses;

	double[] friends;

	double[] followers;

	TweetUsersOfflineAnalyzer(File inFile) throws IOException, ParseException, JSONException {
		List<String> lines = FileUtils.readLines(inFile, Util.UTF8.name());

		this.tweets = new Vector<SimpleTweet>(lines.size());
		this.dates = new long[lines.size()];
		this.statuses = new double[lines.size()];
		this.friends = new double[lines.size()];
		this.followers = new double[lines.size()];

		long lastDate = Long.MIN_VALUE;
		int i = 0;
		for( String line: lines ) {
			String[] tokens = line.split("\t", 6);

			if ( ! (tokens[3].equals("null") && tokens[4].equals("null") && tokens[5].equals("null")) ) {
				
				SimpleTweet tweet = new SimpleTweet(tokens[1]);
				long date = tweet.getCreatedAt().getTime();
				if (date < lastDate) {
					throw new IllegalArgumentException("The tweets are not sorted by increasing date");
				}
				
				int statuscnt, friendcnt, followercnt;

				try {
					statuscnt = Integer.parseInt(tokens[3]);
					friendcnt = Integer.parseInt(tokens[4]);
					followercnt = Integer.parseInt(tokens[5]);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException(line);
				}

				dates[i] = date;
				tweets.add(tweet);
				statuses[i] = (double)statuscnt;
				friends[i] = (double)friendcnt;
				followers[i] = (double)followercnt;
				
				lastDate = date;
				i++;
			}
		}
	}

	private void analyze(File outFile) throws IOException {
		ArrayList<String> lines = new ArrayList<String>(tweets.size());
		lines.add( "Timestamp\tTweets\tMeanStatuses\tMeanFriends\tMeanFollowers\tMedianStatuses\tMedianFriends\tMedianFollowers");
		for( int i=0; i<tweets.size(); i++ ) {
			long date = dates[i];
			
			Mean meanStatuses = new Mean();
			Mean meanFriends = new Mean();
			Mean meanFollowers = new Mean();
			
			meanStatuses.setData( statuses, 0, i+1 );
			meanFriends.setData( friends, 0, i+1 );
			meanFollowers.setData( followers, 0, i+1 );
			
			Median medianStatuses = new Median();
			Median medianFriends = new Median();
			Median medianFollowers = new Median();
			
			medianStatuses.setData( statuses, 0, i+1 );
			medianFriends.setData( friends, 0, i+1 );
			medianFollowers.setData( followers, 0, i+1 );
						
			lines.add( date + "\t" + (i+1)
					+ "\t" + (meanStatuses.evaluate()) + "\t" + (meanFriends.evaluate()) + "\t" + (meanFollowers.evaluate())
					+ "\t" + (medianStatuses.evaluate()) + "\t" + (medianFriends.evaluate()) + "\t" + (medianFollowers.evaluate()));

		}
		FileUtils.writeLines(outFile, lines);
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
		outDirectory.mkdirs();

		File inDir = new File(inDirectoryName);
		File[] listFiles = inDir.listFiles();

		for (int i = 0; i < listFiles.length; i++) {
			File inFile = listFiles[i];

			TweetUsersOfflineAnalyzer t = new TweetUsersOfflineAnalyzer(inFile);
			Util.logDebug(t, "Processing file " + i + "/" + listFiles.length);

			t.analyze(new File(outDirectory, inFile.getName() + "-follower-stats"));

			/*
			 * TimeSeries uniqueTweets = t.computeUniqueTweetsSeries(true); File outFile = new
			 * File(outDirectory, inFile.getName() + "-unique-fraction");
			 * uniqueTweets.writeTo(outFile);
			 */
		}
	}
}
