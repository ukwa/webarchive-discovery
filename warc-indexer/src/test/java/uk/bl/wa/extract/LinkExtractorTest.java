package uk.bl.wa.extract;

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
