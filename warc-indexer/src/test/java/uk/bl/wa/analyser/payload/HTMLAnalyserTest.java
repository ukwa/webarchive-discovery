package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2015 State and University Library, Denmark
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
import static org.junit.Assert.assertNotNull;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecordMetaData;
import org.junit.Test;
import org.junit.Before;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author Toke Eskildsen <te@statsbiblioteket.dk>
 *
 */
public class HTMLAnalyserTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

    // NOTE: The number of extract links is 4. This is technically correct, but one of the links are the empty String.
    //       It is not obvious what the value of keeping track of empty links is, so it should be considered to
    //       remove those links. From a larger perspective, this could be argues for all fields, so maybe the check
    //       for the empty String should be done at the SolrRecord level.
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
        core.put("subject-uri", "NotPresent");
        core.put("ip-address", "192.168.1.10");
        core.put("creation-date", "Invalid");
        core.put("content-type", "text/html");
        core.put("length", Long.toString(SAMPLE.length()));
        core.put("version", "InvalidVersion");
        core.put("absolute-offset", "0");
        ArchiveRecordHeader header = new ARCRecordMetaData("invalid", core);
        SolrRecord solr = new SolrRecord();
        InputStream in = new BufferedInputStream(new FileInputStream(SAMPLE), (int) SAMPLE.length());
        in.mark((int) SAMPLE.length());
        ha.analyse(header, in, solr);
        assertEquals("The number of links should be correct",
                     4, solr.getField(SolrFields.SOLR_LINKS).getValueCount());
    }
}
