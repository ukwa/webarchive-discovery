package uk.bl.wa.indexer;

import static org.junit.Assert.*;

import org.junit.Test;

import uk.bl.wa.indexer.WARCIndexer;

public class WARCIndexerTest {
	
	/**
	 * Check timestamp parsing is working correctly, as various forms exist in the ARCs and WARCs.
	 */
	private static final String TIMESTAMP_12 = "200009200005";
	private static final String TIMESTAMP_14 = "20000920000545";
	private static final String TIMESTAMP_16 = "2000092000054543";
	private static final String TIMESTAMP_17 = "20000920000545439";

	@Test
	public void testExtractYear() {
		assertEquals("2000", WARCIndexer.extractYear(TIMESTAMP_16));
	}
	
	@Test
	public void testParseCrawlDate() {
		assertEquals("2000-09-20T00:05:00Z", WARCIndexer.parseCrawlDate(TIMESTAMP_12));
		assertEquals("2000-09-20T00:05:45Z", WARCIndexer.parseCrawlDate(TIMESTAMP_14));
		assertEquals("2000-09-20T00:05:45Z", WARCIndexer.parseCrawlDate(TIMESTAMP_16));
		assertEquals("2000-09-20T00:05:45Z", WARCIndexer.parseCrawlDate(TIMESTAMP_17));
	}
	
	
	/**
	 * Check URL and extension parsing is robust enough.
	 */
	
	@Test 
	public void testParseExtension() {
		assertEquals("png", WARCIndexer.parseExtension("http://host/image.png"));
		assertEquals("png", WARCIndexer.parseExtension("http://host/this/that/image.parseExtension.png"));
		// FIXME Get some bad extensions from the current Solr server and check we can deal with them
		//fail("Not implemented yet!");
	}
	
	
}
