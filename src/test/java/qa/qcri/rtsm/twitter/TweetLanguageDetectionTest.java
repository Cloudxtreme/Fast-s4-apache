package qa.qcri.rtsm.twitter;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import qa.qcri.rtsm.twitter.TweetLanguageDetection;

public class TweetLanguageDetectionTest {

	private TweetLanguageDetection tld;
	
	@Before
	public void setUp() throws Exception {
	}

	//@Test
	public void testGetLanguage() {
		tld = new TweetLanguageDetection();
		
		//assertEquals(null, tld.getLanguage(null));
		
		//assertEquals(null, tld.getLanguage(" "));
		
		String testTweet1 = "4 tornado warnings entire family is in a town where a tornado warning is in effect  #whatthehell";
		assertEquals("en", tld.getLanguage(testTweet1));
		
		String testtweet2 = "AJも中東大テレコム会社Qテルもカタール政府会社＝米イ国。中東SNSは市民標的マッピングでもあるRT @May_Roma こんなマップがRT @8bit_HORIJUN: アルジャジーラはSNS上のやりとりを独自集計して砲撃の箇所や ";
		assertEquals("ja", tld.getLanguage(testtweet2));
		
		String testTweet3 = "El número de refugiados #sirios registrados se ha duplicado desde septiembre. Más de 440.000 en países vecinos. http://t.co/2Ofq6dT7";
		assertEquals("es", tld.getLanguage(testTweet3));
	}

}
