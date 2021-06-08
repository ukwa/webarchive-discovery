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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.parsers.XMLRootNamespaceParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.TimeLimiter;

/**
 * @author anj
 *
 */
public class XMLAnalyser extends AbstractPayloadAnalyser {
    private static Logger log = LoggerFactory.getLogger( XMLAnalyser.class );

    /** */
    private XMLRootNamespaceParser xrns = new XMLRootNamespaceParser();
    private boolean extractXMLRootNamespace = true;

    public XMLAnalyser() {
    }

    public XMLAnalyser(Config conf) {
    }

    @Override
    public boolean shouldProcess(String mime) {
        if (mime.startsWith("application/xml") || mime.startsWith("text/xml")) {
            return true;
        } else {
            return false;
        }
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
        final long start = System.nanoTime();
        Metadata metadata = new Metadata();
        // Also attempt to grab the XML Root NS:
        if( this.extractXMLRootNamespace ) {
            ParseRunner parser = new ParseRunner( xrns, tikainput, metadata, solr );
            try {
                TimeLimiter.run(parser, 30000L, false);
            } catch( Exception e ) {
                log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
                solr.addParseException("when parsing for XML Root Namespace", e);
            }
            solr.addField( SolrFields.XML_ROOT_NS, metadata.get(XMLRootNamespaceParser.XML_ROOT_NS));
        }
        Instrument.timeRel("WARCPayloadAnalyzers.analyze#total","XMLAnalyzer.analyze", start);
    }

}
