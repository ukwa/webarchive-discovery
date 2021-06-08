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

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.io.ArchiveRecordHeader;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;

/**
 * @author anj
 *
 */
public class FirstBytesAnalyser extends AbstractPayloadAnalyser {
    private static Logger log = LoggerFactory.getLogger( FirstBytesAnalyser.class );

    /** */

    private boolean extractContentFirstBytes = true;
    private int firstBytesLength = 32;

    public FirstBytesAnalyser() {
    }

    public void configure(Config conf) {
        this.extractContentFirstBytes = conf
                .getBoolean("warc.index.extract.content.first_bytes.enabled");
        this.firstBytesLength = conf
                .getInt("warc.index.extract.content.first_bytes.num_bytes");
        log.info("first_bytes config: " + this.extractContentFirstBytes + " "
                + this.firstBytesLength);
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
        final long firstBytesStart = System.nanoTime();
        // Pull out the first few bytes, to hunt for new format by magic:
        try {
            byte[] ffb = new byte[this.firstBytesLength];
            int read = tikainput.read(ffb);
            if (read >= 4) {
                String hexBytes = Hex.encodeHexString(ffb);
                solr.addField(SolrFields.CONTENT_FFB,
                        hexBytes.substring(0, 2 * 4));
                StringBuilder separatedHexBytes = new StringBuilder();
                for (String hexByte : Splitter.fixedLength(2).split(hexBytes)) {
                    separatedHexBytes.append(hexByte);
                    separatedHexBytes.append(" ");
                }
                if (this.extractContentFirstBytes) {
                    solr.addField(SolrFields.CONTENT_FIRST_BYTES,
                            separatedHexBytes.toString().trim());
                }
            }
        } catch (Exception i) {
            log.error(i + ": " + i.getMessage() + ";ffb; " + source + "@"
                    + header.getOffset());
        }
        Instrument.timeRel("WARCPayloadAnalyzers.analyze#total",
                "WARCPayloadAnalyzers.analyze#firstbytes", firstBytesStart);
    }

}
