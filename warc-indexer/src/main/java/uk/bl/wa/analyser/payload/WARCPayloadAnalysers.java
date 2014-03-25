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
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * @author anj
 *
 */
public class WARCPayloadAnalysers {
	private static Log log = LogFactory.getLog( WARCPayloadAnalysers.class );
	
	public static HTMLAnalyser html = new HTMLAnalyser(ConfigFactory.load());

	public static PDFAnalyser pdf = new PDFAnalyser(ConfigFactory.load());

	public static XMLAnalyser xml = new XMLAnalyser(ConfigFactory.load());

	public static ImageAnalyser image = new ImageAnalyser(ConfigFactory.load());

	private boolean extractApachePreflightErrors = true;

	public WARCPayloadAnalysers( Config conf ) {
		this.extractApachePreflightErrors = conf.getBoolean( "warc.index.extract.content.extractApachePreflightErrors" );
	}
	
	public void analyse(ArchiveRecordHeader header, InputStream tikainput, SolrRecord solr) {
		// Entropy, compressibility, fuzzy hashes, etc.
		try {
			tikainput.reset();
			String mime = ( String ) solr.getField( SolrFields.SOLR_CONTENT_TYPE ).getValue();
			if( mime.startsWith( "text" ) || mime.startsWith("application/xhtml+xml") ) {
				WARCPayloadAnalysers.html.analyse(header, tikainput, solr);

			} else if( mime.startsWith( "image" ) ) {
				WARCPayloadAnalysers.image.analyse(header, tikainput, solr);

			} else if( mime.startsWith( "application/pdf" ) ) {
				if( extractApachePreflightErrors ) {
					WARCPayloadAnalysers.pdf.analyse(header, tikainput, solr);
				}
			} else if( mime.startsWith("application/xml") || mime.startsWith("text/xml") ) {
				WARCPayloadAnalysers.xml.analyse(header, tikainput, solr);
				
			} else {
				log.debug("No specific additional parser for: "+mime);
			}
		} catch( Exception i ) {
			log.error( i + ": " + i.getMessage() + ";x; " + header.getUrl() + "@" + header.getOffset() );
		}
		
	}
}
