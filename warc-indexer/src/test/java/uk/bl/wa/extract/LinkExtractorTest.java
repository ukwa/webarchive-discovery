package uk.bl.wa.extract;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2017 The UK Web Archive
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LinkExtractorTest {

    @Test
    public void testExtractPublicSuffixFromHost() {
        // Currently the code treats all UK domains as:
        // <sub>.<private>.<public>.uk
        // which is a bit off for special cases like nhs.uk, bl.uk,
        // parliament.uk
        //
        // Note that any leading /www/ will be stripped by this point
        testExtractPublicSuffixFromHost("news.bbc.co.uk", "bbc.co.uk");
        testExtractPublicSuffixFromHost("bbc.co.uk", "bbc.co.uk");
        testExtractPublicSuffixFromHost("place.nhs.uk", "place.nhs.uk");
        testExtractPublicSuffixFromHost("nhs.uk", "nhs.uk");
        testExtractPublicSuffixFromHost("parliament.uk", "parliament.uk");
    }

    private void testExtractPublicSuffixFromHost(String host,
            String expectedResult) {
        String domain = LinkExtractor
                .extractPrivateSuffixFromHost(host);
        System.err.println("domain " + domain + " from " + host);
        assertEquals(expectedResult, domain);

    }

}
