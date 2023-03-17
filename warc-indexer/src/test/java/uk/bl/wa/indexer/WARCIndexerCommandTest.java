package uk.bl.wa.indexer;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import uk.bl.wa.indexer.WARCIndexerCommandOptions.OutputFormat;
import uk.bl.wa.util.Instrument;

import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.*;

/**
 * Helper class for debugging warc-indexer with local configs & WARCs.
 * The class is bening as it checks for file existence, so it becomes a no-op for people that does
 * not have the test files.
 */
public class WARCIndexerCommandTest {
    private static Logger log = LoggerFactory.getLogger(WARCIndexerCommandTest.class);

    // Local WARC that triggered long exit for the JVM after processing has finished
    @Test
    public void testSBWARC() throws NoSuchAlgorithmException, TransformerException, IOException {
        testWARC("/home/te/projects/netarkivet/warc-indexer-conf-rewrite-url.conf",
                "/home/te/projects/measurements/solrcloud/169568-178-20121224135757-00257-sb-prod-har-006.statsbiblioteket.dk.arc.gz");
        Instrument.log(true);
    }

    // Local WARC that contains problematic harvests of srcSet
    @Test
    public void testSBWARCSrcSet() throws NoSuchAlgorithmException, TransformerException, IOException {
        testWARC("/home/te/projects/measurements/solrcloud/config3.conf",
                "/home/te/projects/netarkivet/354485-265-20210102130246631-00000-sb-prod-har-002.statsbiblioteket.dk.warc.gz");
        Instrument.log(true);
    }

    // Local WARC that contains EXIF which was not indexed
    @Test
    public void testKBWARCExif() throws NoSuchAlgorithmException, TransformerException, IOException {
        testWARC("/home/te/projects/webarchive-discovery/config3_toes.conf",
                "/home/te/projects/webarchive-discovery/katte_gps.warc");
        Instrument.log(true);
    }

    // warcit package from https://github.com/netarchivesuite/solrwayback/issues/192
    @Test
    public void testWarcit() throws NoSuchAlgorithmException, TransformerException, IOException {
        testWARC("/home/te/projects/webarchive-discovery/config3_toes.conf",
                "/home/te/projects/webarchive-discovery/20020308.warc.gz");
        Instrument.log(true);
    }

    private void testWARC(String config, String warc) throws NoSuchAlgorithmException, TransformerException, IOException {
        if (!new File(warc).exists()) {
            log.info("The WARC file '" + warc + "' could not be located. Skipping test");
            return;
        }

        final String TMP = System.getProperty("java.io.tmpdir") + "/";
        assertTrue("The config '" + config + "' should be available", new File(config).exists());

        WARCIndexerCommandOptions opts = new WARCIndexerCommandOptions();
        opts.config = config;
        opts.output = TMP;
        opts.outputFormat = OutputFormat.jsonl;
        opts.inputFiles = new String[]{warc};

        WARCIndexerCommand.parseWarcFiles(opts, false);
    }

}
