package uk.bl.wa.util;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2015 The UK Web Archive
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

import junit.framework.TestCase;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class InstrumentTest extends TestCase {
    private static Log log = LogFactory.getLog(InstrumentTest.class);

    // Not a real test as it requires visual inspection
    public void testHierarchical() {
        Instrument.time("top", 123456);
        Instrument.time("top", "mid1", 123457);
        Instrument.time("top", "mid2", 123459);
        Instrument.time("mid1", "bottom", 123460);
        assertTrue("There should be a double indent in\n" + Instrument.getStats(),                   Instrument.getStats().contains("    "));
    }

}
