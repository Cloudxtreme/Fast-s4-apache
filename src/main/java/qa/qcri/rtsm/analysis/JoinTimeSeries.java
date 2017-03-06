package qa.qcri.rtsm.analysis;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;

import qa.qcri.rtsm.analysis.TimeSeries.EmptySeriesException;
import qa.qcri.rtsm.analysis.TimeSeries.LineParser;
import qa.qcri.rtsm.util.Util;

public class JoinTimeSeries {

	protected static final String SUFFIX_INPUT_VISITS_NAME = ".html.csv-moving-average-reltime";
	
	final File outDirectory;
	final File[] inVisits;
	final File[] allInFacebook;
	final File[] allInTwitter;
	final File[] allInTwitterWordEntropy;
	final File[] allInTwitterUnique;
	final File[] allInTwitterUniqueFraction;
	final File[] allInTwitterCorporateRetweetsFraction;
	final File[] allInSourceO;
	final File[] allInSourceD;
	final File[] allInSourceI;
	final File[] allInSourceR;
	
	HashMap<File,File> facebookMap;
	HashMap<File,File> twitterMap;
	HashMap<File,File> twitterWEMap;
	HashMap<File,File> twitterUMap;
	HashMap<File,File> twitterUFMap;
	HashMap<File,File> twitterCRFMap;
	HashMap<File,File> sourceOMap;
	HashMap<File,File> sourceDMap;
	HashMap<File,File> sourceIMap;
	HashMap<File,File> sourceRMap;
		
	public JoinTimeSeries(String outDirectory, String visitsDirName, String facebookDirName, String twitterDirName, String twitterWordEntropyDirName, String twitterUniqueDirName, String twitterUniqueFractionDirName, String twitterCorporateRetweetsFractionDirName, String sourceODirName, String sourceDDirName, String sourceIDirName, String sourceRDirName) {
		this.outDirectory = new File(outDirectory); 
		inVisits = listFiles(visitsDirName, SUFFIX_INPUT_VISITS_NAME);
		allInFacebook = listFiles(facebookDirName, null);
		allInTwitter = listFiles(twitterDirName, null);
		allInTwitterWordEntropy = listFiles(twitterWordEntropyDirName, null);
		allInTwitterUnique = listFiles(twitterUniqueDirName, null);
		allInTwitterUniqueFraction = listFiles(twitterUniqueFractionDirName, null);
		allInTwitterCorporateRetweetsFraction = listFiles(twitterCorporateRetweetsFractionDirName,null);
		allInSourceO = listFiles(sourceODirName, null);
		allInSourceD = listFiles(sourceDDirName, null);
		allInSourceI = listFiles(sourceIDirName, null);
		allInSourceR = listFiles(sourceRDirName, null);
	}
	
	private String getBasename(File file) {
		String name = file.getName();
		name = name.replaceAll("visits-http___", "");
		name = name.replaceAll("facebook-http___", "");
		name = name.replaceAll("twitter-http___", "");
		name = name.replaceAll("tweets-http___", "");
		name = name.replaceAll("source-o_d_i_r-http___", "");
		name = name.replaceAll(".html.csv-corporate-retweets-fraction-reltime", "");
		name = name.replaceAll(".html.csv-entropy-reltime", "");
		name = name.replaceAll(".html.csv-unique-fraction-reltime", "");
		name = name.replaceAll(".html.csv-unique-reltime", "");
		name = name.replaceAll(".html.csv-moving-average-reltime", "");
		return name;
	}
	
	private File find(File needle, File[] haystack) {
		String basename = getBasename(needle);
		for( File file: haystack) {
			String otherbase = getBasename(file);
			if( basename.equals(otherbase)) {
				return file;
			}
		}
		return null;
	}
	
	private File findAux(File needle, File[] haystack, HashMap<File, File> map) {
		File found = find(needle, haystack);
		if( found != null ) {
			map.put(needle, found);
			//Util.logDebug(this, "Match found: '" + found + "'" );
		}
		return found;
	}
	
	private void mapFileNames() {
		facebookMap = new HashMap<File,File>();
		twitterMap = new HashMap<File,File>();
		twitterWEMap = new HashMap<File,File>();
		twitterUMap = new HashMap<File,File>();
		twitterUFMap = new HashMap<File,File>();
		twitterCRFMap = new HashMap<File,File>();
		sourceOMap = new HashMap<File,File>();
		sourceDMap = new HashMap<File,File>();
		sourceIMap = new HashMap<File,File>();
		sourceRMap = new HashMap<File,File>();

		int nFacebookMatches = 0;
		int nTwitterMatches = 0;
		int nTwitterWEMatches = 0;
		int nTwitterUMatches = 0;
		int nTwitterUFMatches = 0;
		int nTwitterCRFMatches = 0;
		int nSourceOMatches = 0;
		int nSourceDMatches = 0;
		int nSourceIMatches = 0;
		int nSourceRMatches = 0;
		
		int nAllMatches = 0;
		for( File visit: inVisits ) {
			//Util.logDebug(this, "Finding a match for '" + visit + "'");
			
			File facebook = findAux(visit, allInFacebook, facebookMap);
			nFacebookMatches += ( facebook != null ) ? 1 : 0;
			
			File twitter = findAux(visit, allInTwitter, twitterMap);
			nTwitterMatches += ( twitter != null ) ? 1 : 0;
			
			File twitterWE = findAux(visit, allInTwitterWordEntropy, twitterWEMap);
			nTwitterWEMatches += ( twitterWE != null ) ? 1 : 0;
			
			File twitterU = findAux(visit, allInTwitterUnique, twitterUMap);
			nTwitterUMatches += ( twitterU != null ) ? 1 : 0;
			
			File twitterUF = findAux(visit, allInTwitterUniqueFraction, twitterUFMap);
			nTwitterUFMatches += ( twitterUF != null ) ? 1 : 0;
			
			File twitterCRF = findAux(visit, allInTwitterCorporateRetweetsFraction, twitterCRFMap);
			nTwitterCRFMatches += ( twitterCRF != null ) ? 1 : 0;
			
			if( twitter != null && ( twitterWE == null || twitterU == null || twitterUF == null || twitterCRF == null ) ) {
				Util.logWarning(this, "Found a match in twitter series, but not in one of the tweet statistics");
			}

			File sourceO = findAux(visit, allInSourceO, sourceOMap);
			nSourceOMatches += ( sourceO != null ) ? 1 : 0;
			
			File sourceD = findAux(visit, allInSourceD, sourceDMap);
			nSourceDMatches += ( sourceD != null ) ? 1 : 0;
			
			File sourceI = findAux(visit, allInSourceI, sourceIMap);
			nSourceIMatches += ( sourceI != null ) ? 1 : 0;
			
			File sourceR = findAux(visit, allInSourceR, sourceRMap);
			nSourceRMatches += ( sourceR != null ) ? 1 : 0;
				
			if( facebook != null && twitter != null && twitterWE != null && twitterU != null && twitterUF != null && twitterCRF != null && sourceO != null && sourceD != null && sourceI != null && sourceR != null ) {
				nAllMatches++;
			}
			
			Util.logDebug(this, "Files: " + inVisits.length + "; found in Facebook: " + nFacebookMatches
					+ " found in Twitter: "	+ nTwitterMatches
					+ " found in TwitterWE: "	+ nTwitterWEMatches
					+ " found in TwitterU: "	+ nTwitterUMatches
					+ " found in TwitterUF: "	+ nTwitterUFMatches
					+ " found in TwitterCRF: "  + nTwitterCRFMatches
					+ " found in Source: "	+ nSourceOMatches + "/" + nSourceDMatches + "/" + nSourceIMatches + "/" + nSourceRMatches + 
					" ; found in All: " + nAllMatches);
		}
	}

	private void generateOutput() throws IOException, EmptySeriesException {
		for( File visits: inVisits ) {
			if( facebookMap.containsKey(visits) && twitterMap.containsKey(visits) && twitterWEMap.containsKey(visits) && twitterUMap.containsKey(visits) && twitterUFMap.containsKey(visits) && twitterCRFMap.containsKey(visits) && sourceOMap.containsKey(visits) && sourceDMap.containsKey(visits) && sourceIMap.containsKey(visits) && sourceRMap.containsKey(visits) ) {
				Util.logInfo(this, "Generating output for " + visits );
				generateOutput( visits, facebookMap.get(visits), twitterMap.get(visits), twitterWEMap.get(visits), twitterUMap.get(visits), twitterUFMap.get(visits), twitterCRFMap.get(visits), sourceOMap.get(visits), sourceDMap.get(visits), sourceIMap.get(visits), sourceRMap.get(visits) );
			}
		}
	}

	private void generateOutput(File visits, File facebook, File twitter, File twitterWE, File twitterU, File twitterUF, File twitterCRF, File sourceO, File sourceD, File sourceI, File sourceR) throws IOException, EmptySeriesException {
		TimeSeries v = new TimeSeries(visits, new LineParser(), "visits" );
		TimeSeries f = new TimeSeries(facebook, new LineParser(), "facebook" );
		TimeSeries t = new TimeSeries(twitter, new LineParser(), "twitter" );
		TimeSeries twe = new TimeSeries(twitterWE, new LineParser(), "tweets_wordent" );
		TimeSeries tu = new TimeSeries(twitterU, new LineParser(), "tweets_uniq" );
		TimeSeries tuf = new TimeSeries(twitterUF, new LineParser(), "tweets_uniqfrac" );
		TimeSeries tcrf = new TimeSeries(twitterCRF, new LineParser(), "tweets_corprtfrac" );
		TimeSeries so = new TimeSeries(sourceO, new LineParser(), "source-o" );
		TimeSeries sd = new TimeSeries(sourceD, new LineParser(), "source-d" );
		TimeSeries si = new TimeSeries(sourceI, new LineParser(), "source-i" );
		TimeSeries sr = new TimeSeries(sourceR, new LineParser(), "source-r" );
				
		TreeSet<Long> distinctDates = new TreeSet<Long>();
		distinctDates.addAll( v.getDates() );
		distinctDates.addAll( f.getDates() );
		distinctDates.addAll( t.getDates() );
		distinctDates.addAll( twe.getDates() );
		distinctDates.addAll( tu.getDates() );
		distinctDates.addAll( tuf.getDates() );
		distinctDates.addAll( tcrf.getDates() );
		distinctDates.addAll( so.getDates() );
		distinctDates.addAll( sd.getDates() );
		distinctDates.addAll( si.getDates() );
		distinctDates.addAll( sr.getDates() );
		
		File outFile = new File(outDirectory, "joined-" + getBasename(visits) + ".csv" );
		Vector<String> lines = new Vector<String>(distinctDates.size());
		lines.add( "EvTime\tRelEvTime\tVisits\tFacebook\tTwitter\tTweetsWordEntropy\tTweetsUnique\tTweetsUniqueFraction\tTweetsCorporateRetweetsFraction\tSource-O\tSource-D\tSource-I\tSource-R");
		Long firstDate = null;
		for( Long date: distinctDates ) {
			if( firstDate == null ) {
				firstDate = date;
			}
			Long relDate = new Long( date.longValue() - firstDate.longValue() );
			Double vVal = v.containsKey(date) ? v.get(date) : null; 
			Double fVal = f.containsKey(date) ? f.get(date) : null;
			Double tVal = t.containsKey(date) ? t.get(date) : null;
			Double tweVal = twe.containsKey(date) ? twe.get(date) : null;
			Double tuVal = tu.containsKey(date) ? tu.get(date) : null;
			Double tufVal = tuf.containsKey(date) ? tuf.get(date) : null;
			Double tcrfVal = tcrf.containsKey(date) ? tcrf.get(date) : null;
			Double soVal = so.containsKey(date) ? so.get(date) : null;
			Double sdVal = sd.containsKey(date) ? sd.get(date) : null;
			Double siVal = si.containsKey(date) ? si.get(date) : null;
			Double srVal = sr.containsKey(date) ? sr.get(date) : null;
			lines.add( date + "\t" + relDate +  
					"\t" + ( vVal != null ? vVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( fVal != null ? fVal : TimeSeries.LineParser.NULL_VALUE ) + 
					"\t" + ( tVal != null ? tVal : TimeSeries.LineParser.NULL_VALUE ) + 
					"\t" + ( tweVal != null ? tweVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( tuVal != null ? tuVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( tufVal != null ? tufVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( tcrfVal != null ? tcrfVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( soVal != null ? soVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( sdVal != null ? sdVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( siVal != null ? siVal : TimeSeries.LineParser.NULL_VALUE ) +
					"\t" + ( srVal != null ? srVal : TimeSeries.LineParser.NULL_VALUE ) 
					);
		}

		Util.logInfo(this, "Writing file '" + outFile + "'");
		FileUtils.writeLines(outFile, lines);
	}

	/**
	 * Returns all the files in the given directory matching the suffix, or all of them is the suffix is null.
	 * 
	 * @param dirName
	 * @param suffix
	 * @return
	 */
	private File[] listFiles(String dirName, final String suffix) {
		Util.logDebug(this, "Finding files in '" + dirName + "' with suffix '" + suffix + "'");
		File dir = new File(dirName);
		FilenameFilter filenameFilter = new FilenameFilter() {
			@Override
			public boolean accept(File inDir, String name) {
				if( (suffix == null) || (name.endsWith(suffix)) ) {
					return true;
				}
				return false;
			}};
		String[] fileNames = dir.list(filenameFilter);	
		File[] files = new File[fileNames.length];
		for( int i=0; i<fileNames.length; i++ ) {
			files[i] = new File(dirName, fileNames[i]); 
		}
		return files;
	}

	public static void usage(Options options, String message) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(JoinTimeSeries.class.getSimpleName(), "", options, "\n" + message, true);
		System.exit(1);
	}
	
	public static void usage(Options options) {
		usage(options, "");
	}
	
	public static void main(String[] args) throws ParseException, IOException, EmptySeriesException {
		Options options = new Options();
		options.addOption("h", "help", false, "Show this help message");
		options.addOption("o", "outdir", true, "Directory to write to");
		options.addOption("v", "visits", true, "Directory to read visit series from");
		options.addOption("f", "facebook", true, "Directory to read facebook shares series from");
		options.addOption("t", "twitter", true, "Directory to read twitter series from");
		options.addOption(null, "tweets-word-entropy", true, "Directory to read tweets word entry series from");
		options.addOption(null, "tweets-unique", true, "Directory to read tweets unique count series from");
		options.addOption(null, "tweets-unique-fraction", true, "Directory to read tweets unique fraction series from");
		options.addOption(null, "tweets-corporate-retweets-fraction", true, "Directory to read tweets corporate retweets fraction series from");
		options.addOption(null, "source-o", true, "Directory to read source-o (source organic) series from");
		options.addOption(null, "source-d", true, "Directory to read source-d (source direct) series from");
		options.addOption(null, "source-i", true, "Directory to read source-i (source internal) series from");
		options.addOption(null, "source-r", true, "Directory to read source-r (source referal) series from");
		options.addOption(null, "shift-evtime", false, "Shift times before writing, so the first one is zero");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		if( cmd.hasOption("help") ) {
			usage(options);
		}
		if( ! ( cmd.hasOption("outdir") && cmd.hasOption("visits")  && cmd.hasOption("facebook") && cmd.hasOption("twitter") ) ) {
			usage(options);
		}
		
		String outDirectory = cmd.getOptionValue("outdir");
		String visitsDirName = cmd.getOptionValue("visits");
		String facebookDirName = cmd.getOptionValue("facebook");
		String twitterDirName = cmd.getOptionValue("twitter");
		String twitterWEDirName = cmd.getOptionValue("tweets-word-entropy");
		String twitterUDirName = cmd.getOptionValue("tweets-unique");
		String twitterUFDirName = cmd.getOptionValue("tweets-unique-fraction");
		String twitterCRFDirName = cmd.getOptionValue("tweets-corporate-retweets-fraction");
		String sourceODirName = cmd.getOptionValue("source-o");
		String sourceDDirName = cmd.getOptionValue("source-d");
		String sourceIDirName = cmd.getOptionValue("source-i");
		String sourceRDirName = cmd.getOptionValue("source-r");
		
		JoinTimeSeries joiner = new JoinTimeSeries(outDirectory, visitsDirName, facebookDirName, twitterDirName, twitterWEDirName, twitterUDirName, twitterUFDirName, twitterCRFDirName, sourceODirName, sourceDDirName, sourceIDirName, sourceRDirName);
		joiner.mapFileNames();
		joiner.generateOutput();
	}
}
