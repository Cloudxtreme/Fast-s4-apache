package qa.qcri.rtsm.twitter;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TweetNLPFeaturesTest {

        String tweet = "01/06/12 05:12 Temp 74.4°F DP 70.6° Hum 88% Bar. 29.850 inHg Steady,  Wind SSW @ 0 G 1 Rain 0.00, Changeable, mending #txwx";
        TweetNLPFeatures tweetNLPFeatures;

        @Before
        public void setUp() throws Exception {
                tweetNLPFeatures = new TweetNLPFeatures();
        }

        public void test() {
                System.out.println(tweet);
                String features = TweetNLPFeatures.getNLPFeatures(tweet);
                String[] features_split = features.split("\t");
                boolean flag = false;

                if(features_split[3].equals("[txwx]"))
                {
                        flag = true;
                }
                assertTrue(flag);
        }
}
