package qa.qcri.rtsm.analysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;

import qa.qcri.rtsm.analysis.TimeSeries.Point;

public class TimeSeries implements Iterable<Point> {
	
	public static class EmptySeriesException extends Exception {
		private static final long serialVersionUID = 1;
	}
	
	TreeMap<Long, Double> series;
	
	final String label;

	public static class Point implements Entry<Long,Double> {
		private Long key;

		private Double value;

		public Point(Long key, Double value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public Long getKey() {
			return key;
		}

		@Override
		public Double getValue() {
			return value;
		}

		@Override
		public Double setValue(Double value) {
			throw new IllegalStateException("This is immutable");
		}

		@Override
		public boolean equals(Object other) {
			return key.equals(((Point)other).getKey()) && value.equals(((Point)other).getValue());
		}
		
		@Override
		public String toString() {
			return "(" + getKey() + "," + getValue() + ")";
		}
	}

	static class LineParser {
		
		static final String NULL_VALUE = "null";
		
		final static String DEFAULT_FIELD_SEPARATOR = "\t";
		
		int arrayPositionValue = 1;
		
		public LineParser() {
			this(0);
		}
		
		public LineParser(int numField) {
			this.arrayPositionValue = numField + 1;
		}
		
		boolean isNull(String tok) {
			return tok==null || tok.length()==0 || tok.equalsIgnoreCase(NULL_VALUE);
		}

		Point parse(String line) {
			String tokens[] = line.split(DEFAULT_FIELD_SEPARATOR);
			Long date = new Long(Long.parseLong(tokens[0]));
			String tok = tokens[arrayPositionValue];
			if( isNull(tok) ) {
				return null;
			} else {
				Double value = new Double(Double.parseDouble(tok));
				return new Point(date, value);
			}
		}
		
		Point[] parseMulti(String line, int nSeries) {
			String tokens[] = line.split(DEFAULT_FIELD_SEPARATOR, nSeries+1);
			Long date = new Long(Long.parseLong(tokens[0]));
			Point[] result = new Point[nSeries];
			for( int i=0; i<nSeries; i++ ) {
				String tok = tokens[i+1];
				if( isNull(tok) ) {
					result[i] = null;
				} else {
					result[i] = new Point(date, new Double(Double.parseDouble(tok)));
				}
				
			}
			return result;
		}
		
		String format(Point point) {
			return point.getKey() + DEFAULT_FIELD_SEPARATOR + point.getValue();
		}
		
	}
	
	public void insertPoint(Point point) {
		if( series.containsKey(point.getKey())) {
			throw new IllegalArgumentException("Another value for the same key already exists");
		}
		series.put(point.getKey(), point.getValue());
	}
	
	public void insertOrReplacePoint(Point point) {
		series.put(point.getKey(), point.getValue());
	}
	
	public void addPoint(Point point) {
		if( series.containsKey(point.getKey()) ) {
			series.put( point.getKey(), new Double(series.get(point.getKey()).doubleValue() + point.getValue().doubleValue())) ;
		} else {
			insertPoint(point);
		}
	}
	
	public TimeSeries(File inFile, LineParser reader, String label) throws IOException, EmptySeriesException {
		this( FileUtils.readLines(inFile, "UTF-8"), reader, label );
	}
	
	public TimeSeries(List<String> lines, LineParser reader, String label) throws EmptySeriesException {
		this(label);
		
		for (String line : lines) {
			Point point = reader.parse(line);
			if( point != null ) {
				insertPoint(point);
			}
		}
		if( series.size() == 0 ) {
			throw new EmptySeriesException();
		}		
	}
	
	public static ArrayList<TimeSeries> getArrayList(File inFile, LineParser reader, String[] labels, int nSeries) throws IOException {
		List<String> lines = FileUtils.readLines(inFile, "UTF-8");
		
		return getArrayList(lines, reader, labels, nSeries);
		
	}
	
	public static ArrayList<TimeSeries> getArrayList(List<String> lines, LineParser reader, String[] labels, int nSeries) throws IOException {
		// Check input variables
		if( labels.length != nSeries ) {
			throw new IllegalArgumentException("Mismatch between number of labels and number of series");
		}
		
		// Prepare output var
		ArrayList<TimeSeries> tss = new ArrayList<TimeSeries>(nSeries);
		for( int i=0; i<nSeries; i++ ) {
			tss.add(new TimeSeries(labels[i]));
		}

		for(String line: lines) {
			Point[] points = reader.parseMulti(line, nSeries);
			for( int i=0; i<nSeries; i++ ) {
				if( points[i] != null ) {
					tss.get(i).insertPoint(points[i]);
				}
			}
		}
		
		if( tss.size() != nSeries ) {
			throw new IllegalStateException();
		}
		return tss;
	}

	public ArrayList<String> getLines() {
		ArrayList<String> lines = new ArrayList<String>(series.entrySet().size());
		LineParser formatter = new LineParser();
		for( Point point: this ) {
			lines.add( formatter.format(point) );
		}
		return lines;
	}

	public void writeTo(File file) throws IOException {
		ArrayList<String> lines = getLines();
		try {
			FileUtils.writeLines(file, lines);
		} catch( FileNotFoundException e ) {
			e.printStackTrace();
		}
	}
	
	public void dump() {
		ArrayList<String> lines = getLines();
		for( String line: lines ) {
			System.out.println(line);
		}
	}

	public TimeSeries(String label) {
		this.label = label;
		this.series = new TreeMap<Long,Double>();
	}

	public void pointWiseAddition(TimeSeries other) {
		for( Point point: other ) {
			addPoint(point);
		}
	}
	
	public Point firstPoint() {
		Entry<Long, Double> firstEntry = series.firstEntry();
		return new Point( firstEntry.getKey(), firstEntry.getValue() );
	}
	
	public Set<Long> getDates() {
		return series.keySet();
	}
	
	public boolean beginsBefore(long date) {
		return firstPoint().getKey().longValue() < date;
	}
	
	public boolean beginsAfter(long date) {
		return firstPoint().getKey().longValue() > date;
	}
	
	public long countEventsBefore(Long toDateExclusive) {
		long sumCount = 0;
		for( Double count: series.headMap(toDateExclusive).values() ) {
			sumCount += count.longValue();
		}
		return sumCount;
	}
	
	public long countEventsBetween(Long fromDateInclusive, Long toDateExclusive) {
		long sumCount = 0;
		for( Double count: series.subMap(fromDateInclusive, toDateExclusive).values() ) {
			sumCount += count.longValue();
		}
		return sumCount;
	}

	@Override
	public Iterator<Point> iterator() {
		return new Iterator<Point>() {
			
			Iterator<Entry<Long, Double>> iterator;
			
			{
				iterator =  series.entrySet().iterator();
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Point next() {
				Entry<Long, Double> entry = iterator.next();
				return new Point( entry.getKey(), entry.getValue() );
			}

			@Override
			public void remove() {
				throw new NotImplementedException();
			}};
	}
	
	public TimeSeries convertTimeToNumberOfEventsGivenCumulative(TimeSeries cumulative, boolean doNotAdd) {
		TimeSeries newSeries = new TimeSeries( this.label + "-reltime");
		for( Point point: this ) {
			Long floorKey = cumulative.floorKey(point.getKey());
			if( floorKey == null ) {
				Point newPoint = new Point( new Long(0), point.getValue());
				if( doNotAdd ) {
					newSeries.insertOrReplacePoint(newPoint);
				} else {
					newSeries.addPoint(newPoint);
				}
			} else {
				long newDate = cumulative.get(floorKey).longValue();
				Point newPoint = new Point( new Long(newDate), point.getValue());
				if( doNotAdd ) {
					newSeries.insertOrReplacePoint( newPoint );
				} else {
					newSeries.addPoint( newPoint );
				}
			}
		}
		return newSeries;
	}

	Double get(Long key) {
		return series.get(key);
	}
	
	private Long floorKey(Long key) {
		return series.floorKey(key);
	}

	public boolean containsKey(Long date) {
		return series.containsKey(date);
	}
	
	public String getLabel() {
		return label;
	}
	
	public long sumValues() {
		long sum = 0;
		for( Double val: series.values() ) {
			sum += val.longValue();
		}
		return sum;
	}
	
	public int size() {
		return series.size();
	}
	
	public TimeSeries movingAverage(int windowSize, int granularity) {
		MovingAverage<Double> ma = new MovingAverage<Double>(windowSize,granularity);
		SortedMap<Long, Double> result = ma.computeByDuration(series);
		TimeSeries out = new TimeSeries( this.label + "-moving-average");
		for( Entry<Long,Double> entry: result.entrySet() ) {
			out.insertPoint( new Point(entry.getKey(), entry.getValue()));
		}
		return out;
	}
	
	public TimeSeries relativeSeriesIgnoreDescending() {
		TimeSeries relative = new TimeSeries(getLabel() + "-relative");
		double lastValue = Double.MAX_VALUE;
		boolean firstPoint = true;
		for( Entry<Long, Double> point: series.entrySet() ) {
			double currentValue = point.getValue().doubleValue();
			if( currentValue >= lastValue ) {
				relative.insertPoint( new Point( point.getKey(), new Double( currentValue - lastValue ) ) );
			} else if( ! firstPoint ) {
				relative.insertPoint( new Point( point.getKey(), new Double( 0 ) ) );
			}
			lastValue = currentValue;
			firstPoint = false;
		}
		
		return relative;
	}
}
