/**
 * 
 */
package uk.bl.wa.analyser;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2018 The webarchive-discovery project contributors
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

import java.io.InputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.analyser.payload.AbstractPayloadAnalyser;
import uk.bl.wa.analyser.payload.TikaPayloadAnalyser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;


/**
 * 
 * This runs the payload through all the analysers that the ServiceLoader can
 * find.
 * 
 * It runs Tika first, to set up the MIME type, then runs the rest.
 * 
 * TODO Entropy, compressibility, fuzzy hashes, etc. ?
 * 
 * @author anj
 *
 */
public class WARCPayloadAnalysers {
    private static Log log = LogFactory.getLog( WARCPayloadAnalysers.class );
    
    List<AbstractPayloadAnalyser> providers;

    TikaPayloadAnalyser tika = new TikaPayloadAnalyser();

    public WARCPayloadAnalysers(Config conf) {
        // Setup tika:
        tika.configure(conf);
        // And the rest:
        providers = AbstractPayloadAnalyser.getPayloadAnalysers(conf);
    }

    /**
     * 
     * @param source
     * @param header
     * @param tikainput
     * @param solr
     */
    public void analyse(String source, ArchiveRecordHeader header,
            InputStream tikainput, SolrRecord solr) {
        final String url = Normalisation
                .sanitiseWARCHeaderValue(header.getUrl());
        log.debug("Analysing " + url);

        final long start = System.nanoTime();

        // Always run Tika first:
        // (this ensures the SOLR_CONTENT_TYPE is set)
        tika.analyse(source, header, tikainput, solr);

        // Now run the others:
        for (AbstractPayloadAnalyser provider : providers) {
            String mimeType = (String) solr
                    .getField(SolrFields.SOLR_CONTENT_TYPE).getValue();
            if (provider.shouldProcess(mimeType)) {
                try {
                    // Reset input stream before running each parser:
                    tikainput.reset();
                    // Run the parser:
                    provider.analyse(source, header, tikainput, solr);
                } catch (Exception i) {
                    log.error(i + ": " + i.getMessage() + ";x; " + url + "@"
                            + header.getOffset(), i);
                }
            }
        }
        Instrument.timeRel("WARCIndexer.extract#analyzetikainput",
                "WARCPayloadAnalyzers.analyze#total", start);

    }

}
