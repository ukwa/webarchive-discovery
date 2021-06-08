/**
 * 
 */
package uk.bl.wa.util;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.util.ColourMatcher;

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
     * Test method for {@link uk.bl.wa.util.ColourMatcher#getMatch(int, int, int)}.
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
