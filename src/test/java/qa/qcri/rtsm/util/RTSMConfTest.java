package qa.qcri.rtsm.util;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class RTSMConfTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testRTSMConf() {
		assertTrue( (new RTSMConf()) != null );
	}

	@Test
	public void testGetAppName() {
		RTSMConf conf = new RTSMConf();
		assertTrue( conf.getAppName() != null && conf.getAppName().length() > 0 );
	}

}
