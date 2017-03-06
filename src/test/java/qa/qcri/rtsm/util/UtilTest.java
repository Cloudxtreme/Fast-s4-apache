package qa.qcri.rtsm.util;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class UtilTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testLongestCommonSubsequence() {
		assertEquals( "123", Util.longestCommonSubsequence("a1b2c3def", "wx12yz3") );
		assertEquals( "123", Util.longestCommonSubsequence("123def", "wx123") );
		assertEquals( "123", Util.longestCommonSubsequence("1aaaa2bbbbb3def", "wx1yyyyy2z3") );
	}

	@Test
	public void testLongestCommonSubstring() {
		assertEquals( "1", Util.longestCommonSubstring("a1b2c3def", "wx12yz3") );
		assertEquals( "23", Util.longestCommonSubstring("1a23def", "wx1y23z") );
	}

}
