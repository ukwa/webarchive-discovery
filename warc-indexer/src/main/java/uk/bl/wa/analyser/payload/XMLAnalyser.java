/**
 * 
 */
package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2014 The UK Web Archive
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
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.parsers.XMLRootNamespaceParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

import com.typesafe.config.Config;

/**
 * @author anj
 *
 */
public class XMLAnalyser extends AbstractPayloadAnalyser {
	private static Log log = LogFactory.getLog( XMLAnalyser.class );

	/** */
	private XMLRootNamespaceParser xrns = new XMLRootNamespaceParser();
	private boolean extractXMLRootNamespace = true;

	public XMLAnalyser(Config conf) {
	}

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(ArchiveRecordHeader header, InputStream tikainput,
			SolrRecord solr) {
		Metadata metadata = new Metadata();
		// Also attempt to grab the XML Root NS:
		if( this.extractXMLRootNamespace ) {
			ParseRunner parser = new ParseRunner( xrns, tikainput, metadata, solr );
			Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
			try {
				thread.start();
				thread.join( 30000L );
				thread.interrupt();
			} catch( Exception e ) {
				log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
				solr.addParseException("when parsing for XML Root Namespace", e);
			}
			solr.addField( SolrFields.XML_ROOT_NS, metadata.get(XMLRootNamespaceParser.XML_ROOT_NS));
		}
	}

}
