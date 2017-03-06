package qa.qcri.rtsm.twitter;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import qa.qcri.rtsm.util.RTSMConf;

import twitter4j.Status;

/**
 * Utility to serialize a set of tweets, used mostly for development.
 * 
 * @author chato
 *
 */
public class TwitterSearchSerializer { 
	
	public static void main( String[] args ) throws ParseException, IOException, ClassNotFoundException {
		Option optQuery = new Option("q", "query", true, "query to issue");
		optQuery.setRequired(true);
		Option optOutfile = new Option("o", "outfile", true, "File to store output to");
		optQuery.setRequired(true);
		
		Options options = new Options();
		options.addOption( optQuery );
		options.addOption( optOutfile );
				
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		
		// Issue query
		String query = cmd.getOptionValue("query");
		TwitterSearcher ts = new TwitterSearcher(RTSMConf.TEST_TWITTER_CONSUMER_KEY, RTSMConf.TEST_TWITTER_CONSUMER_SECRET, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN, RTSMConf.TEST_TWITTER_OAUTH_ACCESS_TOKEN_SECRET);
		Vector<Status> results = ts.search( query ).getTweets();
		assertTrue( results.size() > 0 );
		System.err.println( "Tweets found: " + results.size() );
		
		// Serialize to outfile
		String outfile = cmd.getOptionValue("outfile");
		writeTweets( results, outfile );
		System.err.println( "Tweets saved to: " + outfile );
		
		Vector<Status> readBack = readTweets(outfile);
		System.err.println( "Verified: " + readBack.size() + " tweets are in the file");
	}

	static Vector<Status> readTweets(String outfile) throws IOException, ClassNotFoundException {
		FileInputStream fis = new FileInputStream(outfile);
		ObjectInputStream in = new ObjectInputStream(fis);
		@SuppressWarnings("unchecked")
		Vector<Status> read = (Vector<Status>) in.readObject();
		in.close();
		return read;
	}
	
	static void writeTweets(Vector<Status> results, String outfile) throws IOException {
		FileOutputStream fos = new FileOutputStream( outfile );
		ObjectOutputStream out = new ObjectOutputStream(fos);
		out.writeObject(results);
		out.close();
	}

}
