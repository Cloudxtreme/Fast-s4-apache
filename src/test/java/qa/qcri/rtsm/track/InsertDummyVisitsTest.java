package qa.qcri.rtsm.track;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class InsertDummyVisitsTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testRandomSource() {
		assertTrue( InsertDummyVisits.randomSource().length() > 0 );
	}

	@Test
	public void testRandomSearchTerms() {
		assertTrue( InsertDummyVisits.randomSearchTerms().length() > 0 );
	}

}
