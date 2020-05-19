/**
 * 
 */
package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.archive.io.ArchiveRecordHeader;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;

import picocli.CommandLine.Option;
import uk.bl.wa.nanite.droid.DroidDetector;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;
import uk.gov.nationalarchives.droid.command.action.CommandExecutionException;

/**
 * @author anj
 *
 */
public class DroidDetectorAnalyser extends AbstractPayloadAnalyser {
    private static Log log = LogFactory.getLog( DroidDetectorAnalyser.class );

    /** */
    private DroidDetector dd = null;

    @Option(names = "--no-droid", negatable = true, defaultValue = "true")
    private boolean runDroid = true;

    @Option(names = "--no-droid-ext-hint", negatable = true, defaultValue = "false")
    private boolean passUriToFormatTools = false;

    private boolean droidUseBinarySignaturesOnly = false;

    @Option(names = "--droid-binsig-only", defaultValue = "false")
    public void setDroidUseBinarySignaturesOnly(boolean binSigOnly) {
        this.droidUseBinarySignaturesOnly = binSigOnly;
        dd.setBinarySignaturesOnly(droidUseBinarySignaturesOnly);
    }

    public DroidDetectorAnalyser() {
        // Attempt to set up Droid:
        try {
            dd = new DroidDetector();
        } catch (CommandExecutionException e) {
            e.printStackTrace();
            dd = null;
        }
        Instrument.createSortedStat("WARCPayloadAnalyzers.analyze#droid",
                Instrument.SORT.avgtime, 5);
    }

    @Override
    public boolean shouldProcess(String mime) {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.
     * ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
     */
    @Override
    public void analyse(String source, ArchiveRecordHeader header, InputStream tikainput,
            SolrRecord solr) {

        // Also run DROID (restricted range):
        if (dd != null && runDroid == true) {
            final long droidStart = System.nanoTime();
            try {
                // Pass the URL in so DROID can fall back on that:
                Metadata metadata = new Metadata();
                if (passUriToFormatTools) {
                    UsableURI uuri = UsableURIFactory.getInstance(
                            Normalisation.fixURLErrors(
                                    Normalisation.sanitiseWARCHeaderValue(header.getUrl())));
                    // Droid seems unhappy about spaces in filenames, so hack to
                    // avoid:
                    String cleanUrl = uuri.getName().replace(" ", "+");
                    metadata.set(Metadata.RESOURCE_NAME_KEY, cleanUrl);
                }
                // Run Droid:
                MediaType mt = dd.detect(tikainput, metadata);
                solr.addField(SolrFields.CONTENT_TYPE_DROID, mt.toString());
                Instrument.timeRel("WARCPayloadAnalyzers.analyze#droid",
                        "WARCPayloadAnalyzers.analyze#droid_type="
                                + mt.toString(),
                        droidStart);
            } catch (Exception i) {
                // Note that DROID complains about some URLs with an
                // IllegalArgumentException.
                log.error(i + ": " + i.getMessage() + ";dd; " + source + " @"
                        + header.getOffset(), i);
            }
            Instrument.timeRel("WARCPayloadAnalyzers.analyze#total",
                    "WARCPayloadAnalyzers.analyze#droid", droidStart);

        }
    }

}
