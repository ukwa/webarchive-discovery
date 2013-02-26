/**
 * 
 */
package uk.bl.wap.util;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ColourMatcherTest {

	ColourMatcher cm = null;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		cm = new ColourMatcher();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link uk.bl.wap.util.ColourMatcher#getMatch(int, int, int)}.
	 */
	@Test
	public void testGetMatchIntIntInt() {
		testGetMatchIntIntInt("blue",0,0,255);
		testGetMatchIntIntInt("navy",0,0,128);
		testGetMatchIntIntInt("red",255,0,0);
		testGetMatchIntIntInt("aquamarine", 126,254,211);
	}

	/**
	 * 
	 * @param name
	 * @param r
	 * @param g
	 * @param b
	 */
	private void testGetMatchIntIntInt(String name, int r, int g, int b ) {
		String testName = cm.getMatch(r,g,b).getName();
		assertEquals("Failed to match colour "+name+", got "+testName, name, testName );
	}

}
