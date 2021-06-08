/**
 * 
 */
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

import java.io.InputStream;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.parsers.ApachePreflightParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;
import uk.bl.wa.util.TimeLimiter;

/**
 * @author anj
 *
 */
public class PDFAnalyser extends AbstractPayloadAnalyser {
    private static Logger log = LoggerFactory.getLogger( PDFAnalyser.class );

    /** */
    private ApachePreflightParser app = new ApachePreflightParser();

    private boolean extractApachePreflightErrors;

    public PDFAnalyser() {
    }

    public PDFAnalyser(Config conf) {
    }

    public void configure(Config conf) {
        this.extractApachePreflightErrors = conf.getBoolean(
                "warc.index.extract.content.extractApachePreflightErrors");
    }

    @Override
    public boolean shouldProcess(String mime) {
        if (mime.startsWith("application/pdf")) {
            if (extractApachePreflightErrors) {
                return true;
            }
        }
        return false;
    }

    /* (non-Javadoc)
     * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
     */
    @Override
    public void analyse(String source, ArchiveRecordHeader header,
            InputStream tikainput,
            SolrRecord solr) {
        final long start = System.nanoTime();
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, Normalisation.sanitiseWARCHeaderValue(header.getUrl()));
        ParseRunner parser = new ParseRunner(app, tikainput, metadata, solr);
        try {
            TimeLimiter.run(parser, 30000L, false);
        } catch (Exception e) {
            log.error("WritableSolrRecord.extract(): " + e.getMessage());
            solr.addParseException("when parsing with Apache Preflight", e);
        }

        String isValid = metadata
                .get(ApachePreflightParser.PDF_PREFLIGHT_VALID);
        solr.addField(SolrFields.PDFA_IS_VALID, isValid);
        String[] errors = metadata
                .getValues(ApachePreflightParser.PDF_PREFLIGHT_ERRORS);
        // The same errors can occur multiple times, but in this context that
        // count is not terribly useful. We just want to know which errors are
        // present in which resources.
        if (errors != null) {
            // Add found errors to a Set, counting them up as we go:
            HashMap<String, Integer> uniqueErrors = new HashMap<String, Integer>();
            for (String error : errors) {
                if (uniqueErrors.containsKey(error)) {
                    uniqueErrors.put(error, uniqueErrors.get(error) + 1);
                } else {
                    uniqueErrors.put(error, 0);

                }
            }
            // Store the Set as the result (the count is ignored at present):
            for (String error : uniqueErrors.keySet()) {
                solr.addField(SolrFields.PDFA_ERRORS, error);
            }
        }
        Instrument.timeRel("WARCPayloadAnalyzers.analyze#total",
                           "PDFAnalyzer.analyze", start);
    }

}
