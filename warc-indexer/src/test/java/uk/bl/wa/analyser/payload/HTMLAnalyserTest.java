package uk.bl.wa.analyser.payload;

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecordMetaData;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;

/**
 * @author Toke Eskildsen <te@statsbiblioteket.dk>
 *
 */
public class HTMLAnalyserTest {

    // NOTE: The number of extract links is 4. This is correct as the empty
    // String should be discarded.
    @Test
    public void testLinksExtraction() throws IOException {
        final URL SAMPLE_RESOURCE = Thread.currentThread().getContextClassLoader().getResource("links_extract.html");
        assertNotNull("The sample file should be resolved", SAMPLE_RESOURCE);
        final File SAMPLE = new File(SAMPLE_RESOURCE.getFile());

        final URL CONF_RESOURCE = Thread.currentThread().getContextClassLoader().getResource("links_extract.conf");
        assertNotNull("The config file should be resolved", CONF_RESOURCE);
        final File CONF = new File(CONF_RESOURCE.getFile());
        Config config = ConfigFactory.parseFile(CONF);
        HTMLAnalyser ha = new HTMLAnalyser(config);
        Map<String, Object> core = new HashMap<String, Object>();
        core.put("subject-uri", "http://example.org/");
        core.put("ip-address", "192.168.1.10");
        core.put("creation-date", "Invalid");
        core.put("content-type", "text/html");
        core.put("length", Long.toString(SAMPLE.length()));
        core.put("version", "InvalidVersion");
        core.put("absolute-offset", "0");
        core.put("origin", "");
        ArchiveRecordHeader header = new ARCRecordMetaData("invalid", core);
        SolrRecord solr = SolrRecordFactory.createFactory(null).createRecord();
        InputStream in = new BufferedInputStream(new FileInputStream(SAMPLE), (int) SAMPLE.length());
        in.mark((int) SAMPLE.length());
        ha.analyse("source", header, in, solr);

        // Check number of links:
        assertEquals("The number of links should be correct", 6,
                solr.getField(SolrFields.SOLR_LINKS).getValueCount());

        // Check hosts are canonicalized:
        assertEquals("The number of hosts should be correct. Got hosts " + solr.getField(SolrFields.SOLR_LINKS_HOSTS),
                     1, solr.getField(SolrFields.SOLR_LINKS_HOSTS).getValueCount());
        String host = (String) solr.getField(SolrFields.SOLR_LINKS_HOSTS)
                .getFirstValue();
        assertEquals("The host should be formatted correctly", "example.org",
                host);
        // The domains and suffixes too:
        String domain = (String) solr.getField(SolrFields.SOLR_LINKS_DOMAINS)
                .getFirstValue();
        assertEquals("The domain should be formatted correctly", "example.org",
                domain);
        String suffix = (String) solr.getField(
                SolrFields.SOLR_LINKS_PUBLIC_SUFFIXES).getFirstValue();
        assertEquals("The suffix should be formatted correctly", "org",
                suffix);

        // Check links_domains_surts
        String[] linksDomainsSurts = new String[]{"(org,", "(org,example,"};
        assertArrayEquals("The SURT domains should be correct", linksDomainsSurts,
                solr.getField(SolrFields.SOLR_LINKS_HOSTS_SURTS).getValues()
                        .toArray());

        assertEquals("Image links should be for both src and srcset",
                     12, solr.getField(SolrFields.SOLR_LINKS_IMAGES).getValueCount());
    }
}
