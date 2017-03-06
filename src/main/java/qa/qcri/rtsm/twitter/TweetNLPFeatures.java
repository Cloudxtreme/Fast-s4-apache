package qa.qcri.rtsm.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import qa.qcri.rtsm.nlpTools.TweetAnnotation;
import qa.qcri.rtsm.nlpTools.TweetAnnotator;

public class TweetNLPFeatures {
	
	private static TweetAnnotator tweetAnnotator = new TweetAnnotator();
	
	public static String getNLPFeatures(String tweet) {
		String NLPFeatures = "";
		TweetAnnotation tweetAnnotation = tweetAnnotator.annotate(tweet);
		NLPFeatures += tweetAnnotation.dates + "\t";
		NLPFeatures += tweetAnnotation.locations + "\t";
		NLPFeatures += tweetAnnotation.mentions + "\t";
		NLPFeatures += tweetAnnotation.hashtags + "\t";
		NLPFeatures += tweetAnnotation.names;
		return NLPFeatures;
	}
	
	public static void main(String[] args) throws IOException {
		String tweet = "01/06/12 05:12 Temp 74.4°F DP 70.6° Hum 88% Bar. 29.850 inHg Steady,  Wind SSW @ 0 G 1 Rain 0.00, Changeable, mending #txwx";
		
		String features = getNLPFeatures(tweet);
		System.out.println(features);
	}
}
