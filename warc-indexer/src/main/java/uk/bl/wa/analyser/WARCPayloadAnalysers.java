/**
 * 
 */
package uk.bl.wa.analyser;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.mime.MediaType;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.analyser.payload.AbstractPayloadAnalyser;
import uk.bl.wa.analyser.payload.TikaPayloadAnalyser;
import uk.bl.wa.indexer.HTTPHeader;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.InputStreamUtils;
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
    private static Logger log = LoggerFactory.getLogger(WARCPayloadAnalysers.class );
    
    List<AbstractPayloadAnalyser> providers;

    TikaPayloadAnalyser tika = new TikaPayloadAnalyser();

    public WARCPayloadAnalysers(Config conf) {
        // Setup tika:
        tika.configure(conf);
        // And the rest:
        providers = AbstractPayloadAnalyser.getPayloadAnalysers(conf);
    }

    /**
     *  @param source
     * @param header
     * @param httpHeader
     * @param tikainput
     * @param solr
     */
    public void analyse(String source, ArchiveRecordHeader header, HTTPHeader httpHeader,
                        InputStream tikainput, SolrRecord solr, long content_length) {
        // Note: The repeated use of InputStreamUtils.maybeDecompress might cause multiple uncompressions.
        // This is acceptable as it saves memory/temporary disk space and because the compression schemes
        // used are GZip & Brotly, both of which are fairly light weight to decompress.
        final String url = Normalisation.sanitiseWARCHeaderValue(header.getUrl());
        log.debug("Analysing " + url);

        final long start = System.nanoTime();

        // Always run Tika first:
        // (this ensures the SOLR_CONTENT_TYPE is set)
        try {
            tika.analyse(source, header, tikainput, solr);
        } catch (Exception e) {
            log.error("IOException analyzing content of '" + source + "' with tika", e);
        }

        // Now run the others:
        for (AbstractPayloadAnalyser provider : providers) {
            String mimeType = (String) solr.getField(SolrFields.SOLR_CONTENT_TYPE).getValue();
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

        // Derive normalised/simplified content type:
        processContentType(solr, header, content_length, false);

        // End
        Instrument.timeRel("WARCIndexer.extract#analyzetikainput",
                "WARCPayloadAnalyzers.analyze#total", start);

    }

    /**
     * 
     * @param solr
     * @param header
     * @param content_length
     */
    private void processContentType(SolrRecord solr, ArchiveRecordHeader header,
            long content_length, boolean revisit) {
        // Get the current content-type:
        String contentType = (String) solr
                .getFieldValue(SolrFields.SOLR_CONTENT_TYPE);

        // Store the raw content type from Tika:
        solr.setField(SolrFields.CONTENT_TYPE_TIKA, contentType);

        // Also get the other content types:
        MediaType mt_tika = MediaType.parse(contentType);
        if (solr.getField(SolrFields.CONTENT_TYPE_DROID) != null) {
            MediaType mt_droid = MediaType.parse((String) solr
                    .getField(SolrFields.CONTENT_TYPE_DROID).getFirstValue());
            if (mt_tika == null || mt_tika.equals(MediaType.OCTET_STREAM)) {
                contentType = mt_droid.toString();
            } else if (mt_droid.getBaseType().equals(mt_tika.getBaseType())
                    && mt_droid.getParameters().get("version") != null) {
                // Union of results:
                mt_tika = new MediaType(mt_tika, mt_droid.getParameters());
                contentType = mt_tika.toString();
            }
            if (mt_droid.getParameters().get("version") != null) {
                solr.addField(SolrFields.CONTENT_VERSION,
                        mt_droid.getParameters().get("version"));
            }
        }

        // Allow header MIME
        if (contentType != null && contentType.isEmpty()) {
            if (header.getHeaderFieldKeys()
                    .contains("WARC-Identified-Payload-Type")) {
                contentType = ((String) header.getHeaderFields()
                        .get("WARC-Identified-Payload-Type"));
            } else {
                contentType = header.getMimetype();
            }
        }
        // Determine content type:
        if (contentType != null)
            solr.setField(SolrFields.FULL_CONTENT_TYPE, contentType);

        // If zero-length, then change to application/x-empty for the
        // 'content_type' field.
        if (content_length == 0 && !revisit)
            contentType = "application/x-empty";

        // Content-Type can still be null
        if (contentType != null) {
            // Strip parameters out of main type field:
            solr.setField(SolrFields.SOLR_CONTENT_TYPE,
                    contentType.replaceAll(";.*$", ""));

            // Also add a more general, simplified type, as appropriate:
            if (contentType.matches("^image/.*$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "image");
                solr.setField(SolrFields.SOLR_TYPE, "Image");
            } else if (contentType.matches("^audio/.*$")
                    || contentType.matches("^application/vnd.rn-realaudio$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "audio");
                solr.setField(SolrFields.SOLR_TYPE, "Audio");
            } else if (contentType.matches("^video/.*$")
                    || contentType.matches("^application/mp4$")
                    || contentType.matches("^application/vnd.rn-realmedia$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "video");
                solr.setField(SolrFields.SOLR_TYPE, "Video");
            } else if (contentType.matches("^text/htm.*$")
                    || contentType.matches("^application/xhtml.*$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "html");
                solr.setField(SolrFields.SOLR_TYPE, "Web Page");
            } else if (contentType.matches("^application/pdf.*$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "pdf");
                solr.setField(SolrFields.SOLR_TYPE, "Document");
            } else if (contentType.matches("^.*word$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "word");
                solr.setField(SolrFields.SOLR_TYPE, "Document");
            } else if (contentType.matches("^.*excel$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "excel");
                solr.setField(SolrFields.SOLR_TYPE, "Data");
            } else if (contentType.matches("^.*powerpoint$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE,
                        "powerpoint");
                solr.setField(SolrFields.SOLR_TYPE, "Presentation");
            } else if (contentType.matches("^text/plain.*$")) {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "text");
                solr.setField(SolrFields.SOLR_TYPE, "Document");
            } else {
                solr.setField(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "other");
                solr.setField(SolrFields.SOLR_TYPE, "Other");
            }

            // Remove text from JavaScript, CSS, ...
            if (contentType.startsWith("application/javascript")
                    || contentType.startsWith("text/javascript")
                    || contentType.startsWith("text/css")) {
                solr.removeField(SolrFields.SOLR_EXTRACTED_TEXT);
            }
        }
    }

}
