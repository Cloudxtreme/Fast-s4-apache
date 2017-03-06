package qa.qcri.rtsm.analysis;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import qa.qcri.rtsm.analysis.TimeSeries.EmptySeriesException;
import qa.qcri.rtsm.analysis.TimeSeries.LineParser;
import qa.qcri.rtsm.analysis.TimeSeries.Point;
import qa.qcri.rtsm.util.Util;

public class ConvertTimeSeries {
	private static final String SUFFIX_INPUT_FILES = ".csv";
	private static final String INFIX_INPUT_FILES = ".csv-";
	private static final String FILENAME_SUM_SERIES = "sum_series.csv";
	private static final String FILENAME_CUMULATIVE_SERIES = "cumulative_series.csv";
	
	TimeSeries[] inSeries;
	final File outDirectory;
	
	public ConvertTimeSeries(String inDirectoryName, String outDirectoryName, int numSeries) throws IOException {

		// Get list of files
		File dir = new File(inDirectoryName);
		Util.logInfo(this, "Reading directory '" + dir + "'");
		FilenameFilter filenameFilter = new FilenameFilter() {
			@Override
			public boolean accept(File inDir, String name) {
				if( name.endsWith(SUFFIX_INPUT_FILES) || name.contains(INFIX_INPUT_FILES) ) {
					return true;
				}
				return false;
			}};
		String[] inFileNames = dir.list(filenameFilter);
		if( inFileNames == null ) {
			throw new IllegalArgumentException("Couldn't find the directory '" + inDirectoryName + "'");
		} else if( inFileNames.length == 0 ) {
			throw new IllegalArgumentException("Couldn't find files with suffix '" + SUFFIX_INPUT_FILES + "' or infix '" + INFIX_INPUT_FILES + "' in the directory '" + inDirectoryName + "'");
		}

		// Read time series, skipping empty files
		Vector<TimeSeries> series = new Vector<TimeSeries>(inFileNames.length);
		for( int i=0; i<inFileNames.length; i++ ) {
			TimeSeries ts;
			try {
				ts = new TimeSeries( new File(dir, inFileNames[i]), new LineParser(numSeries), inFileNames[i] );
				series.add(ts);
			} catch( EmptySeriesException e ) {
				Util.logWarning(this, "Skipping empty series in file '" + inFileNames[i] + "'" );
			}
		}
		
		inSeries = series.toArray( new TimeSeries[] {} );
		this.outDirectory = new File(outDirectoryName);
	}
	
	public ConvertTimeSeries(String inDirectoryName, String outDirectoryName) throws IOException {
		this(inDirectoryName, outDirectoryName, 0);
	}
	
	public ConvertTimeSeries(TimeSeries[] aSeries) {
		this.inSeries = aSeries;
		this.outDirectory = null;
	}

	abstract class Filter {
		abstract boolean accepts(TimeSeries ts);
	}
	
	abstract class Converter {
		abstract TimeSeries convert(TimeSeries ts);
	}
	
	private void applyFilter(Filter filter) {
		Util.logDebug(this, "Number of series before filtering: " + inSeries.length );
		Vector<TimeSeries> newInSeries = new Vector<TimeSeries>(inSeries.length);
		for( TimeSeries ts: inSeries ) {
			if( filter.accepts(ts) ) {
				newInSeries.add(ts);
			}
		}
		inSeries = newInSeries.toArray(new TimeSeries[newInSeries.size()]);
		Util.logDebug(this, "Number of series after filtering: " + inSeries.length );
	}
	
	private void applyConverter(Converter converter) {
		Vector<TimeSeries> newInSeries = new Vector<TimeSeries>(inSeries.length);
		int count = 0;
		for( TimeSeries ts: inSeries ) {
			count++;
			newInSeries.add( converter.convert(ts) );
			if( count % 100 == 0 ) {
				Util.logDebug(this, "... series " + count + "/" + inSeries.length);
			}
		}
		inSeries = newInSeries.toArray(new TimeSeries[newInSeries.size()]);
	}
			
	private void removeSeriesBeginningAfter(final long date) {
		Util.logInfo(this, "Removing series that begin after " + date );
		
		applyFilter(new Filter() {
			@Override
			boolean accepts(TimeSeries ts) {
				return ( ! ts.beginsAfter(date) );
			}});
	}
	
	private void removeSeriesBeginningBefore(final long date) {
		Util.logInfo(this, "Removing series that begin before " + date );
		
		applyFilter(new Filter() {
			@Override
			boolean accepts(TimeSeries ts) {
				return ( ! ts.beginsBefore(date) );
			}});
	}
	
	private void removeSeriesWithFewerEvents(final int minSum) {
		Util.logInfo(this, "Removing series that have less than " + minSum + " events in total" );
		applyFilter(new Filter() {
			@Override
			boolean accepts(TimeSeries ts) {
				return ( ts.sumValues() >= minSum );
			}});
	}
	
	private void convertToMovingAverage(final int windowSize, final int granularity) {
		Util.logInfo(this, "Transforming series by taking moving average on window of size " + windowSize + " at granularity " + granularity );
		
		applyConverter(new Converter() {

			@Override
			TimeSeries convert(TimeSeries ts) {
				return ts.movingAverage(windowSize, granularity);
			}} );
	}
	
	public TimeSeries generateSumSeries() throws IOException {
		Util.logDebug(this, "Generating sum series");
		TimeSeries sumSeries = new TimeSeries("sum");
		int count = 0;
		for( TimeSeries ts: inSeries ) {
			sumSeries.pointWiseAddition(ts);
			count++;
			if( count % 100 == 0 ) {
				Util.logDebug(this, "... series " + count + "/" + inSeries.length);
			}
		}
		return sumSeries;
	}
	
	public TimeSeries generateCumulativeEventSeries() {
		Util.logDebug(this, "Generating cumulative series 1/2: list of distinct dates");
		// Brute-force
		TreeSet<Long> distinctDates = new TreeSet<Long>();
		for( TimeSeries ts: inSeries ) {
			for( Long date: ts.getDates() ) {
				distinctDates.add(date);
			}
		}
		
		Util.logDebug(this, "Generating cumulative series 2/2: counting");
		TimeSeries eventsBefore = new TimeSeries("events-before");
		int countDates = 0;
		Long lastDate = new Long(Long.MIN_VALUE);
		int count = 0;
		for( Long date: distinctDates ) {
			for( TimeSeries ts: inSeries ) {
				count += ts.countEventsBetween(lastDate, date);
			}
			eventsBefore.insertPoint(new Point(date, new Double(count)));
			
			countDates++;
			if( countDates % 1000 == 0 ) {
				Util.logDebug(this, "... date " + countDates + " of " + distinctDates.size());
			}
			lastDate = date;
		}
		
		return eventsBefore;
	}
	
	public void write(TimeSeries ts, String fileName) throws IOException {
		ts.writeTo(new File(outDirectory,fileName));
	}

	private void writeTimeAsNumberOfEventsBefore(TimeSeries cumulative, boolean doNotAdd) throws IOException {
		if( doNotAdd ) {
			Util.logInfo(this, "While converting time, we will keep the last point only if multiple points fall in the same place (do-not-add)");
		}
		
		Util.logInfo(this, "Converting time to event number using cumulative series 1/2: convert");
		Vector<TimeSeries> convertedSeries = new Vector<TimeSeries>(inSeries.length);
		for( int i=0; i<inSeries.length; i++) {
			convertedSeries.add(i, inSeries[i].convertTimeToNumberOfEventsGivenCumulative(cumulative,doNotAdd) );

			if( i % 100 == 0 ) {
				Util.logDebug(this, "... series " + i + "/" + inSeries.length);
			}
		}
		
		Util.logInfo(this, "Converting time to event number using cumulative series 2/2: write");
		for( int i=0; i<convertedSeries.size(); i++) {
			convertedSeries.get(i).writeTo(new File(outDirectory, convertedSeries.get(i).getLabel()));
			if( i % 100 == 0 ) {
				Util.logDebug(this, "... series " + i + "/" + convertedSeries.size());
			}
		}
	}
	
	public static void usage(Options options, String message) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(ConvertTimeSeries.class.getSimpleName(), "", options, "\n" + message, true);
		System.exit(1);
	}
	
	public static void usage(Options options) {
		usage(options, "");
	}

	public static void main( String[] args ) throws ParseException, IOException {
		Options options = new Options();
		options.addOption("h", "help", false, "Show this help message");
		options.addOption("i", "indir", true, "Directory to read from (created using DataExport)");
		options.addOption("n", "num-series", true, "The number of the time series to process, in the case of multiple time series in a file");
		options.addOption("o", "outdir", true, "Directory to write to");

		// Convert input data
		options.addOption(null, "not-beginning-before", true, "Do not include series that begin before this date");
		options.addOption(null, "not-beginning-after", true, "Do not include series that begin after this date");
		options.addOption(null, "min-events", true, "Do not include series that have less than this number of events");
		options.addOption(null, "moving-average-window-size-millis", true, "Smoothen each input series by computing moving average of this window size" );
		options.addOption(null, "moving-average-granularity-millis", true, "Smoothen each input series by computing moving average at this granularity" );
		
		// Write output data
		options.addOption(null, "write-sum", false, "Write a sum of the input time series");
		OptionGroup groupComputeOrRead = new OptionGroup();
		groupComputeOrRead.addOption(new Option(null, "compute-cumulative", false, "Compute a cumulative series from the input time series"));
		groupComputeOrRead.addOption(new Option(null, "read-cumulative", true, "Read a cumulative series from the given file"));
		options.addOptionGroup(groupComputeOrRead);
		options.addOption(null, "write-cumulative", false, "Write the cumulative series to an output file");
		options.addOption(null, "convert-time-using-cumulative", false, "Create time series with time relative to number of events");
		options.addOption(null, "convert-time-do-not-add", false, "When converting keep the last point (do not add) if multiple points fall in the same place" );
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		
		if( cmd.hasOption("help") ) {
			usage(options);
		}
		
		if( ! cmd.hasOption("indir") ) {
			usage(options, "Expected --indir directory");
		}
		if( ! cmd.hasOption("outdir") ) {
			usage(options, "Expected --outdir directory");
		}

		String inDirectoryName = cmd.getOptionValue("indir");
		String outDirectoryName = cmd.getOptionValue("outdir");
		
		ConvertTimeSeries b;
		if( cmd.hasOption("num-series") ) {
			b = new ConvertTimeSeries(inDirectoryName, outDirectoryName, Integer.parseInt( cmd.getOptionValue("num-series") ) );			
		} else {
			b = new ConvertTimeSeries(inDirectoryName, outDirectoryName);
		}

		
		// Operations than transform the input set
		if( cmd.hasOption("not-beginning-before") ) {
			b.removeSeriesBeginningBefore( Long.parseLong(cmd.getOptionValue("not-beginning-before")) );
		}
		if( cmd.hasOption("not-beginning-after") ) {
			b.removeSeriesBeginningAfter( Long.parseLong(cmd.getOptionValue("not-beginning-after")) );
		}
		if( cmd.hasOption("min-events") ) {
			b.removeSeriesWithFewerEvents( Integer.parseInt(cmd.getOptionValue("min-events")) );
		}
		if( cmd.hasOption("moving-average-window-size-millis") && cmd.hasOption("moving-average-granularity-millis") ) {
			int windowSize = Integer.parseInt(cmd.getOptionValue("moving-average-window-size-millis"));
			int granularity = Integer.parseInt(cmd.getOptionValue("moving-average-granularity-millis"));
			b.convertToMovingAverage(windowSize, granularity);
		}
		
		// Operations for writing
		if( cmd.hasOption("write-sum") ) {
			TimeSeries sum = b.generateSumSeries();
			Util.logInfo(b, "Writing sum series to '" + FILENAME_SUM_SERIES + "'");
			b.write(sum, FILENAME_SUM_SERIES);
		}
		
		TimeSeries cumulative = null;
		
		if( cmd.hasOption("compute-cumulative") && cmd.hasOption("read-cumulative") ) {
			throw new IllegalArgumentException("Can not use --compute-cumulative and --read-cumulative simultaneously");
		} else if( cmd.hasOption("compute-cumulative") ) {
			Util.logInfo(b, "Computing cumulative series");
			cumulative = b.generateCumulativeEventSeries();
		} else if( cmd.hasOption("read-cumulative") ) {
			Util.logInfo(b, "Reading '" + cmd.getOptionValue("read-cumulative") + "'");
			try {
				cumulative = new TimeSeries(new File(cmd.getOptionValue("read-cumulative")), new LineParser(), "cumulative");
			} catch( EmptySeriesException e ) {
				throw new IllegalArgumentException("The time series is empty");
			}
		}

		if( cmd.hasOption("write-cumulative") ) {
			if( cumulative == null ) {
				throw new IllegalArgumentException("Must indicate --compute-cumulative or --read-cumulative");
			}
			Util.logInfo(b, "Writing cumulative series to '" + FILENAME_CUMULATIVE_SERIES + "'");
			b.write(cumulative, FILENAME_CUMULATIVE_SERIES);
		}

		if( cmd.hasOption("convert-time-using-cumulative") ) {
			if( cumulative == null ) {
				throw new IllegalArgumentException("Must indicate --compute-cumulative or --read-cumulative");
			}
			boolean doNotAdd = false;
			if( cmd.hasOption("convert-time-do-not-add")) {
				doNotAdd = true;
			}
			b.writeTimeAsNumberOfEventsBefore(cumulative, doNotAdd);
		}
	}
}