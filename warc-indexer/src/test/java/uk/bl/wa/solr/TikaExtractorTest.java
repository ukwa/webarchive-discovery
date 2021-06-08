/**
 * 
 */
package uk.bl.wa.solr;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.ConfigFactory;

import uk.bl.wa.analyser.payload.TikaPayloadAnalyser;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TikaExtractorTest {
    private static Logger log = LoggerFactory.getLogger(TikaExtractorTest.class);

    private TikaPayloadAnalyser tika;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        tika = new TikaPayloadAnalyser();
        tika.configure(ConfigFactory.load());
    }

    @Test
    public void testMonaLisa() throws Exception {
        File ml = new File("src/test/resources/wikipedia-mona-lisa/Mona_Lisa.html");
        if (!ml.exists()) {
            log.error("The Mona Lisa test file '" + ml + "' does not exist");
            return;
        }
        URL url = ml.toURI().toURL();
        SolrRecord solr = SolrRecordFactory.createFactory(null).createRecord();
        tika.extract(ml.getPath(), solr, url.openStream(), url.toString());
        System.out.println("SOLR " + solr.getSolrDocument().toString());
        String text = (String) solr.getField(SolrFields.SOLR_EXTRACTED_TEXT)
                .getValue();
        assertTrue("Text should contain this string!",
                text.contains("Mona Lisa"));
        assertFalse(
                "Text should NOT contain this string! (implies bad newline handling)",
                text.contains("encyclopediaMona"));
    }

}
