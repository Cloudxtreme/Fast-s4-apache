/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package qa.qcri.rtsm.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import qa.qcri.rtsm.analysis.imran.TimeSeriesSorted;

/**
 *
 * @author Imran
 */
public class ExportData {

    /**
     * This method generates a csv file based on the given timeseries present in the TimeSeriesSorted object.
     * @param timeSeries the instance of TimeSeriesSorted class
     * @param fileName a complete path or only a file name can be provided. In case file name the CSV file will be created in the default location which is the project directory.
     */
    public void exportToCSV(TimeSeriesSorted timeSeries, String fileName) {

        try {
            FileWriter writer = new FileWriter(fileName+"_timeseries.csv");
            writer.append(timeSeries.getKey() + "\n");
            writer.append("timestamp(unix)");
            writer.append(',');
            writer.append("timestamp");
            writer.append(',');
            writer.append("absolute-value");
            writer.append(',');
            writer.append("relative-value");
            writer.append(',');
            writer.append("normalized-value");
            writer.append('\n');
            String result = "";

            SortedMap<Long, Integer> absoluteSeries = null;
            SortedMap<Long, Integer> relativeSeries = null;
            SortedMap<Long, Float> normalizedSeries = null;
            if (timeSeries.getAbsoluteSeries() != null) {
                absoluteSeries = timeSeries.getAbsoluteSeries();
            }
            if (timeSeries.getRelativeSeries() != null) {
                relativeSeries = timeSeries.getRelativeSeries();
            }
            if (timeSeries.getNormalizedSeries() != null) {
                normalizedSeries = timeSeries.getNormalizedSeries();
            }

            if (absoluteSeries != null && relativeSeries != null && normalizedSeries != null) {
                for (Map.Entry<Long, Integer> entry : absoluteSeries.entrySet()) {
                    result += entry.getKey() + "," + getDateTime(entry.getKey()) + "," + entry.getValue() + ","
                            + relativeSeries.get(entry.getKey()) + "," + normalizedSeries.get(entry.getKey()) + "\n";
                }
            } else if (relativeSeries != null && normalizedSeries != null) {
                for (Map.Entry<Long, Integer> entry : relativeSeries.entrySet()) {
                    result += entry.getKey() + "," + getDateTime(entry.getKey()) + "," + "N/A" + ","
                            + relativeSeries.get(entry.getKey()) + "," + normalizedSeries.get(entry.getKey()) + "\n";
                }
            } else if (absoluteSeries != null && relativeSeries != null) {
                for (Map.Entry<Long, Integer> entry : absoluteSeries.entrySet()) {
                    result += entry.getKey() + "," + getDateTime(entry.getKey()) + "," + entry.getValue() + ","
                            + relativeSeries.get(entry.getKey()) + "," + "N/A" + "\n";
                }
            } else if (relativeSeries != null) {
                for (Map.Entry<Long, Integer> entry : relativeSeries.entrySet()) {
                    result += entry.getKey() + "," + getDateTime(entry.getKey()) + "," + "N/A" + ","
                            + relativeSeries.get(entry.getKey()) + "," + "N/A" + "\n";
                }
            } else if (absoluteSeries != null) {
                for (Map.Entry<Long, Integer> entry : absoluteSeries.entrySet()) {
                    result += entry.getKey() + "," + getDateTime(entry.getKey()) + "," + entry.getValue() + ","
                            + "N/A" + "," + "N/A" + "\n";
                }
            }
            writer.write(result);
            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    private String getDateTime(long input) {
        DateTime time = new DateTime(input, DateTimeZone.forTimeZone(TimeZone.getTimeZone("AST")));
        return time.toString();

    }
}
