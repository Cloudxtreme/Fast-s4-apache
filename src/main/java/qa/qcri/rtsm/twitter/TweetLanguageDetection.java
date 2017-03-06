package qa.qcri.rtsm.twitter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;

import qa.qcri.rtsm.util.RTSMConf;
import qa.qcri.rtsm.util.Util;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;

public class TweetLanguageDetection {

	Detector detector;
	// public static final String filename = "sample.txt";
	
	public TweetLanguageDetection() {
		try
		{
			// DetectorFactory.loadProfile(getClass().getClassLoader().getResource("profiles").getFile());
			DetectorFactory.loadProfile(RTSMConf.LANGUAGE_PROFILES_DIR);
		}
		catch (LangDetectException e)
		{
			// e.printStackTrace();
		}
	}
	
	public String getLanguage(String tweet)
	{
		String language = null;
		try
		{
			detector = DetectorFactory.create();
			detector.append(tweet);
			language = detector.detect();
		}

		catch (LangDetectException e)
		{
			e.printStackTrace();
			language = e.getMessage();
			// Util.logDebug(this, "Tweet language: " + e.getMessage());
			// return e.getMessage();
		}
		return language;
	}

}

