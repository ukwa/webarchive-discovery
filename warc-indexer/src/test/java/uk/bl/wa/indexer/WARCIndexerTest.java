package uk.bl.wa.indexer;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2014 The UK Web Archive
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.util.ArchiveUtils;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

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
        this.testFilterBehaviour(path, protocols, 29);
        protocols.remove("http");
        this.testFilterBehaviour(path, protocols, 34);
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
        this.testFilterBehaviour(path, url_excludes, 29);
        url_excludes.add("robots.txt");
        this.testFilterBehaviour(path, url_excludes, 30);

    }

    /**
     * Test reponse code filtering is working ok.
     *
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws MalformedURLException
     */
    @Test
    public void testResponseCodeFilters() throws NoSuchAlgorithmException, MalformedURLException, IOException {
        // Now URL excludes:
        String path = "warc.index.extract.response_include";
        List<String> response_includes = new ArrayList<String>();
        this.testFilterBehaviour(path, response_includes, 36);
        response_includes.add("2");
        this.testFilterBehaviour(path, response_includes, 29);
        response_includes.add("3");
        this.testFilterBehaviour(path, response_includes, 20);
    }

    /**
     * @throws MalformedURLException
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    @Test
    public void testExclusionFilter() throws MalformedURLException, NoSuchAlgorithmException, IOException {
        Config config = ConfigFactory.load();
        // Enable excusion:
        config = this.modifyValueAt(config, "warc.index.exclusions.enabled", true);
        // config exclusion file:
        String inputFile = this.getClass().getClassLoader()
                .getResource("exclusion_test.txt").getPath();
        File exclusions_file = new File(inputFile);
        assertEquals(true, exclusions_file.exists());
        config = this.modifyValueAt(config, "warc.index.exclusions.file",
                exclusions_file.getAbsolutePath());
        // config check interval:
        config = this.modifyValueAt(config, "warc.index.exclusions.check_interval", 600);

        // And run the trial:
        this.testFilterBehaviourWithConfig(config, 32);
    }

	/* ------------------------------------------------------------ */
	
	/*
	 * Internal implementations of filter test core methods.
	 */
	
	/* ------------------------------------------------------------ */

    private void testFilterBehaviour(String path, Object newValue, int expectedNullCount) throws MalformedURLException, IOException, NoSuchAlgorithmException {
        // Override the config:
        Config config = ConfigFactory.load();
        Config config2 = this.modifyValueAt(config, path, newValue);
        // And invoke:
        this.testFilterBehaviourWithConfig(config2, expectedNullCount);
    }

    private Config modifyValueAt(Config config, String path, Object newValue) {
        ConfigValue value = ConfigValueFactory.fromAnyRef(newValue);
        return config.withValue(path, value);
    }

    private void testFilterBehaviourWithConfig(Config config2, int expectedNullCount) throws MalformedURLException, IOException, NoSuchAlgorithmException {
        // Instanciate the indexer:
        WARCIndexer windex = new WARCIndexer(config2);
        windex.setCheckSolrForDuplicates(false);

        String inputFile = this.getClass().getClassLoader()
                .getResource("IAH-urls-wget.warc.gz")
                .getPath();
        System.out.println("ArchiveUtils.isGZipped: " + ArchiveUtils.isGzipped(new FileInputStream(inputFile)));
        ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
        Iterator<ArchiveRecord> ir = reader.iterator();
        int recordCount = 0;
        int nullCount = 0;

        // Iterate though each record in the WARC file
        while(ir.hasNext()) {
            ArchiveRecord rec = ir.next();
            SolrRecord doc = windex.extract("", rec);
            recordCount++;
            if(doc == null) {
                nullCount++;
            }
            else {
                // System.out.println("DOC: " + doc.toXml());
            }
        }

        System.out.println("recordCount: " + recordCount);
        assertEquals(36, recordCount);

        System.out.println("nullCount: " + nullCount);
        assertEquals(expectedNullCount, nullCount);
    }

    @Test
    public void testFields() throws NoSuchAlgorithmException, IOException {
        // ID of WARC record to use in test
        final String recordId = "id=jbKtN3dWzLJzaIQxTyPCiA==/20131021215312";

        WARCIndexer windex = new WARCIndexer(ConfigFactory.load());
        windex.setCheckSolrForDuplicates(false);

        String inputFile = this.getClass().getClassLoader()
                .getResource("IAH-urls-wget.warc.gz").getPath();
        System.out.println("ArchiveUtils.isGZipped: " + ArchiveUtils.isGzipped(new FileInputStream(inputFile)));
        ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
        Iterator<ArchiveRecord> ir = reader.iterator();

        // Iterate though each record in the WARC file
        while(ir.hasNext()) {
            ArchiveRecord rec = ir.next();
            SolrRecord doc = windex.extract("", rec);

            if(doc != null && doc.getField(SolrFields.ID).getValue() == recordId) {
                assertArrayEquals("links_domains_surts is incorrect", new String[]{
                                "(org,",
                                "(org,archive,",
                                "(org,archive,blog,",
                                "(org,archive,web,",
                                "(org,archive,web,faq,",
                                "(org,openlibrary,",
                                "(org,sloan,"
                        },
                        doc.getField(SolrFields.SOLR_LINKS_DOMAINS_SURTS).getValues().toArray());

                assertArrayEquals("domain_surt is incorrect", new String[]{
                                "(org,",
                                "(org,archive,"
                        },
                        doc.getField(SolrFields.SOLR_DOMAIN_SURT).getValues().toArray());

                assertEquals("url_path is incorrect", "/", doc.getField(SolrFields.SOLR_URL_PATH).getValue());

                assertEquals("status_code is incorrect", "200", doc.getField(SolrFields.SOLR_STATUS_CODE).getValue());

                break;
            }
        }
    }
}
