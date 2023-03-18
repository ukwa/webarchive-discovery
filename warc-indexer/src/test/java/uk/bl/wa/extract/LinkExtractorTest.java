package uk.bl.wa.extract;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2023 The webarchive-discovery project contributors
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

import org.apache.commons.httpclient.URIException;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;
import org.junit.Test;
import uk.bl.wa.util.Normalisation;

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

    @Test
    public void testExtractHost() {
        final String[][] TESTS = new String[][]{
                // url, host
                {"http://foo.example.com/", "foo.example.com"},
                {"http://87.com/", "87.com"},
                {"http://a.com/", "a.com"},
                {"http://b-a", "b-a"},
//                {"http://æblegrød.dk", "æblegrød.dk"}, // TODO: Should this be converted to punycode?

                {"http://-a", LinkExtractor.MALFORMED_HOST},
                {"http://abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcd.com", LinkExtractor.MALFORMED_HOST}, // 64 characters in a single part
                {"http://foo.example.com&foo=bar", LinkExtractor.MALFORMED_HOST}
        };
        for (String[] test: TESTS) {
            assertEquals(test[1], LinkExtractor.extractHost(test[0]));
        };
    }

    private void testExtractPublicSuffixFromHost(String host,
            String expectedResult) {
        String domain = LinkExtractor
                .extractPrivateSuffixFromHost(host);
        System.err.println("domain " + domain + " from " + host);
        assertEquals(expectedResult, domain);

    }

    // What is a domain? Answer: It depends. Not just on country-level (the uk-system), but also on individual services.
    @Test
    public void testExtractDomainFromFullURL() throws URIException {
        final String[][] TESTS = new String[][]{
                // url, host, domain
                {"http://fourth.whatever.example.com/",    "fourth.whatever.example.com",    "example.com"},
                {"http://fourth.whatever.googleapis.com/", "fourth.whatever.googleapis.com", "whatever.googleapis.com"},
                {"http://fourth.whatever.cloudfront.net",  "fourth.whatever.cloudfront.net", "whatever.cloudfront.net"},
                {"http://fourth.whatever.blogspot.dk/",    "fourth.whatever.blogspot.dk",    "whatever.blogspot.dk"}
        };

        for (String[] test: TESTS) {
            UsableURI url = UsableURIFactory.getInstance(test[0]);

            String host = url.getHost();
            String canonHost = Normalisation.canonicaliseHost(host);
            assertEquals("The URL '" + test[0] + "' should have the correct host extracted", test[1], canonHost);
            
            final String domain = LinkExtractor.extractPrivateSuffixFromHost(canonHost);
            assertEquals("The URL '" + test[0] + "' should have the correct domain extracted", test[2], domain);
        }
    }

}
