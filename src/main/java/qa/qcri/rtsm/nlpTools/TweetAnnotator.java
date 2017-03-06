package qa.qcri.rtsm.nlpTools;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.twitter.Extractor;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class TweetAnnotator {
	//
	private TweetAnnotation tweetAnnotation;
	private final StanfordCoreNLP pipeline;
	//
	private static final String ANNOTATOR = "annotators";
	private static final String PROPERTIES = "tokenize, ssplit, pos, lemma, ner, parse, dcoref";
	private static final String DATE_TYPE = "DATE";
	private static final String TIME_TYPE = "TIME";
	private static final String NN_TYPE = "NN";
	private static final String LOC_TYPE = "LOCATION";
	//
	public TweetAnnotator() {
		// creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution 
		Properties props = new Properties();
		props.put(ANNOTATOR, PROPERTIES);
		pipeline = new StanfordCoreNLP(props);
	}

	public TweetAnnotation annotate(String text)
	{
		tweetAnnotation = new TweetAnnotation();
		ArrayList<String> namesArray = new ArrayList<String>();
		ArrayList<String> locationsArray = new ArrayList<String>();
		//
		// create an empty Annotation just with the given text
		Annotation document = new Annotation(text);

		// run all Annotators on this text
		pipeline.annotate(document);

		// using com.twitter package from twitter-text-java-master library to extract hashtags & mentiones
		Extractor extractor = new Extractor();
		ArrayList<String> hashtagsList = (ArrayList<String>)extractor.extractHashtags(text);
		ArrayList<String> mentionesList = (ArrayList<String>)extractor.extractMentionedScreennames(text);
		tweetAnnotation.hashtags = hashtagsList;
		tweetAnnotation.mentions = mentionesList;

		// these are all the sentences in this document
		// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		//
		for(CoreMap sentence: sentences) {
			// traversing the words in the current sentence
			// a CoreLabel is a CoreMap with additional token-specific methods
			StringBuffer dateStringBuffer = new StringBuffer();
			StringBuffer nameSeqStringBuffer = new StringBuffer();
			for (int i=0; i<sentence.get(TokensAnnotation.class).size(); i++) {
				CoreLabel token = sentence.get(TokensAnnotation.class).get(i);
				// this is the text of the token
				String word = token.get(TextAnnotation.class);
				// this is the POS tag of the token
				String pos = token.get(PartOfSpeechAnnotation.class);
				// this is the NER label of the token
				String ne = token.get(NamedEntityTagAnnotation.class);
				//System.out.println(word+", "+pos+", "+ne);
				//
				if (ne.equals(DATE_TYPE) || ne.equals(TIME_TYPE)) {
					dateStringBuffer.append(word+" ");
				}
				if (ne.equals(LOC_TYPE)) {
					locationsArray.add(word);
				}//
				if (pos.equals(NN_TYPE) || pos.startsWith(NN_TYPE)) {
					if(!isHashTagOrMention(word)) {
						if (i != 0) {
							CoreLabel preToken = sentence.get(TokensAnnotation.class).get(i-1);
							String prePos = preToken.get(PartOfSpeechAnnotation.class);
							if (prePos.equals(NN_TYPE) || prePos.startsWith(NN_TYPE)) {
								nameSeqStringBuffer.append(" "+word);
							}
							else {
								if(nameSeqStringBuffer.length() !=0)
									namesArray.add(nameSeqStringBuffer.toString());
								nameSeqStringBuffer = new StringBuffer();
								nameSeqStringBuffer.append(word);
							}
						}
						else {// i=0
							nameSeqStringBuffer.append(word);
						}
					}
				}
			}
			// adding last item
			if(nameSeqStringBuffer.length() !=0)
				namesArray.add(nameSeqStringBuffer.toString());
			//
			tweetAnnotation.names = namesArray;
			tweetAnnotation.locations = locationsArray;
			//
			String dateString = dateStringBuffer.toString();
			boolean isNumber = this.isNumber(dateString);
			//
			ArrayList<Date> dates = null;
			if(!(dateString.isEmpty()) && (dateString.length() != 0) && !(isNumber)) {
				dates = this.parseDate(text, dateString);
			}
			tweetAnnotation.dates = dates;
		}
		//
		return tweetAnnotation;
	}

	private boolean isHashTagOrMention(String word) {
		if (word.startsWith("@") || word.startsWith("#")) {
			return true;
		}
		else {
			return false;
		}
	}

	private ArrayList<Date> parseDate(String text, String dateString) {
		ArrayList<Date> datesArray = null;
		// Start Natty date parser
		Parser parser = new Parser();
		List<DateGroup> groups = parser.parse(text);
		for(DateGroup group:groups) {
			// the matching value from Natty-Date Parser
			String matchingValue = group.getText();
			//
			if(dateString.contains(matchingValue)) {
				List<Date> dates = group.getDates();
				datesArray = (ArrayList<Date>) dates;
			}
		}
		return datesArray;
	}

	private boolean isNumber(String dateString) {
		//
		String aString = dateString.replaceAll("\\s+","");
		//
		try {
			Integer.parseInt(aString);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
}
