package qa.qcri.rtsm.util;

import java.nio.charset.Charset;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Util {
	
	static { 
		BasicConfigurator.configure();
	}

	public final static Charset UTF8 = Charset.forName("UTF-8");

	public static void setLogLevel(Object obj, Level level) {
		Logger.getLogger(obj.getClass().getSimpleName()).setLevel(level);
	}
	
	public static void logError(Object obj, String value) {
		Logger.getLogger(obj.getClass().getSimpleName()).error(value);
	}

	public static void logWarning(Object obj, String value) {
		Logger.getLogger(obj.getClass().getSimpleName()).warn(value);
	}
	
	public static void logInfo(Object obj, String value) {
		Logger.getLogger(obj.getClass().getSimpleName()).info(value);
	}
	
	public static void logDebug(Object obj, String value) {
		Logger.getLogger(obj.getClass().getSimpleName()).debug(value);
	}
	
	public static void logTrace(Object obj, String value) {
		Logger.getLogger(obj.getClass().getSimpleName()).trace(value);
	}
	
	
	/**
	 * Source: http://introcs.cs.princeton.edu/java/96optimization/LCS.java.html
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	public static String longestCommonSubsequence(String x, String y) {
		int M = x.length();
		int N = y.length();

		// opt[i][j] = length of LCS of x[i..M] and y[j..N]
		int[][] opt = new int[M + 1][N + 1];

		// compute length of LCS and all subproblems via dynamic programming
		for (int i = M - 1; i >= 0; i--) {
			for (int j = N - 1; j >= 0; j--) {
				if (x.charAt(i) == y.charAt(j))
					opt[i][j] = opt[i + 1][j + 1] + 1;
				else
					opt[i][j] = Math.max(opt[i + 1][j], opt[i][j + 1]);
			}
		}

		StringBuffer buf = new StringBuffer();
		// recover LCS itself and print it to standard output
		int i = 0, j = 0;
		while (i < M && j < N) {
			if (x.charAt(i) == y.charAt(j)) {
				buf.append(x.charAt(i));
				i++;
				j++;
			} else if (opt[i + 1][j] >= opt[i][j + 1])
				i++;
			else
				j++;
		}
		return buf.toString();
	}

	/**
	 * 
	 * Source:  http://karussell.wordpress.com/2011/04/14/longest-common-substring-algorithm-in-java/
	 * 
	 * @param str1
	 * @param str2
	 * @return
	 */
	public static String longestCommonSubstring(String str1, String str2) {

		StringBuilder sb = new StringBuilder();
		if (str1 == null || str1.isEmpty() || str2 == null || str2.isEmpty())
			return "";

		// java initializes them already with 0
		int[][] num = new int[str1.length()][str2.length()];
		int maxlen = 0;
		int lastSubsBegin = 0;

		for (int i = 0; i < str1.length(); i++) {
			for (int j = 0; j < str2.length(); j++) {
				if (str1.charAt(i) == str2.charAt(j)) {
					if ((i == 0) || (j == 0))
						num[i][j] = 1;
					else
						num[i][j] = 1 + num[i - 1][j - 1];

					if (num[i][j] > maxlen) {
						maxlen = num[i][j];
						// generate substring from str1 => i
						int thisSubsBegin = i - num[i][j] + 1;
						if (lastSubsBegin == thisSubsBegin) {
							// if the current LCS is the same as the last time this block ran
							sb.append(str1.charAt(i));
						} else {
							// this block resets the string builder if a different LCS is found
							lastSubsBegin = thisSubsBegin;
							sb = new StringBuilder();
							sb.append(str1.substring(lastSubsBegin, i + 1));
						}
					}
				}
			}
		}

		return sb.toString();
	}
}
