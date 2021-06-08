package uk.bl.wa.util;

/*
 * #%L
 * warc-indexer
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

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentTest extends TestCase {
    private static Logger log = LoggerFactory.getLogger(InstrumentTest.class);

    // Not a real test as it requires visual inspection
    public void testHierarchical() {
        Instrument.clear();
        Instrument.time("top", 123456);
        Instrument.time("top", "mid1", 123457);
        Instrument.time("top", "mid2", 123459);
        Instrument.time("mid1", "bottom", 123460);
        assertTrue("There should be a double indent in\n" + Instrument.getStats(),
                   Instrument.getStats().contains("    "));
        log.info(Instrument.getStats());
    }

    public void testChildTimeSortAndLimit() {
        Instrument.clear();
        Instrument.createSortedStat("top", Instrument.SORT.time, 2);
        Instrument.time("top", 123456);
        Instrument.time("top", "mid1", 925457);
        Instrument.time("top", "mid2", 134459);
        Instrument.time("top", "mid2", 334459);
        Instrument.time("top", "mid3", 145500);
        Instrument.time("top", "mid3", 145500);
        Instrument.time("top", "mid3", 145500);
        assertTrue("Sanity check: The output should contain 'mid2'", Instrument.getStats().contains("mid2"));
        assertFalse("The output should not contain 'mid3' as it is the fastest and the order is 'time' with limit 2",
                    Instrument.getStats().contains("mid3"));
        log.info(Instrument.getStats());
    }

    public void testTimeSortIntOverflow() {
        Instrument.clear();
        Instrument.createSortedStat("top", Instrument.SORT.time, 9999);
        Instrument.setAbsolute("top", "a", 69560000L, 2);
        Instrument.setAbsolute("top", "b", 24590000L, 1);
        Instrument.setAbsolute("top", "c", 16830910000L, 1224);
        Instrument.setAbsolute("top", "d", 12452050000L, 1129);
        log.info(Instrument.getStats());
    }

    public void testChildTimeSortOrder() {
        Instrument.clear();
        Instrument.createSortedStat("top", Instrument.SORT.time, 9999);
        Instrument.time("top", 123456);
        Instrument.time("top", "mid1", 925457);
        Instrument.time("top", "mid2", 134459);
        Instrument.time("top", "mid3", 445500);
        Instrument.time("top", "mid5", 245500);
        
        log.info(Instrument.getStats());
    }

    public void testChildCountSortAndLimit() {
        Instrument.clear();
        Instrument.createSortedStat("top", Instrument.SORT.count, 2);
        Instrument.time("top", 123456);
        Instrument.time("top", "mid1", 925457);
        Instrument.time("top", "mid2", 134459);
        Instrument.time("top", "mid2", 334459);
        Instrument.time("top", "mid3", 145500);
        Instrument.time("top", "mid3", 145500);
        Instrument.time("top", "mid3", 145500);
        assertTrue("Sanity check: The output should contain 'mid2'", Instrument.getStats().contains("mid2"));
        assertFalse("The output should not contain 'mid1': It occurs only once  and the order is 'count' with limit 2",
                    Instrument.getStats().contains("mid1"));
        log.info(Instrument.getStats());
    }

}
