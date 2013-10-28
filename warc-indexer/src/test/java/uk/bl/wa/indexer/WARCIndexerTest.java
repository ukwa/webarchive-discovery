package uk.bl.wa.indexer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.util.solr.SolrRecord;

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
	
	/**
	 * Test protocol filtering is working ok.
	 * 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	@Test
	public void testProtocolFilters() throws NoSuchAlgorithmException, MalformedURLException, IOException {
		// Check protocol excludes:
		String path = "warc.index.extract.protocol_include";
		List<String> protocols = new ArrayList<String>();
		protocols.add("http");
		protocols.add("https");
		this.testFilterBehaviour(path, protocols, 16);
		protocols.remove("http");
		this.testFilterBehaviour(path, protocols, 23);
	}
		
	/**
	 * Test URL filtering is working ok.
	 * 
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException 
	 * @throws MalformedURLException 
	 */
	@Test
	public void testUrlFilters() throws NoSuchAlgorithmException, MalformedURLException, IOException {
		// Now URL excludes:
		String path = "warc.index.extract.url_exclude";
		List<String> url_excludes = new ArrayList<String>();
		this.testFilterBehaviour(path, url_excludes, 16);
		url_excludes.add("robots.txt");
		this.testFilterBehaviour(path, url_excludes, 17);		
		
	}


	/*
	 * Internal implementation of filter test core.
	 */
	private void testFilterBehaviour(String path, Object newValue, int expectedNullCount ) throws MalformedURLException, IOException, NoSuchAlgorithmException {
		// Override the config:
		Config config = ConfigFactory.load();
		ConfigValue value = ConfigValueFactory.fromAnyRef( newValue );
		Config config2 = config.withValue(path, value);
		
		// Instanciate the indexer:
		WARCIndexer windex = new WARCIndexer(config2);
		
		String inputFile = "src/test/resources/IAH-20080430204825-00000-blackbook-truncated.warc.gz";
		ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
		Iterator<ArchiveRecord> ir = reader.iterator();
		int recordCount = 0;
		int nullCount = 0;
		
		// Iterate though each record in the WARC file
		while( ir.hasNext() ) {
			ArchiveRecord rec = ir.next();
			SolrRecord doc = windex.extract("",rec, false);
			recordCount++;
			if( doc == null ) {
				nullCount++;
			}
		}
		
		System.out.println("recordCount: "+recordCount);
		assertEquals(23, recordCount);
		
		System.out.println("nullCount: "+nullCount);
		assertEquals(expectedNullCount, nullCount);
	}
}
