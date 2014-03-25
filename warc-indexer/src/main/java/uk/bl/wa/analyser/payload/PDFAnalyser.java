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

import com.typesafe.config.Config;

import uk.bl.wa.parsers.ApachePreflightParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public class PDFAnalyser extends AbstractPayloadAnalyser {
	private static Log log = LogFactory.getLog( PDFAnalyser.class );

	/** */
	private ApachePreflightParser app = new ApachePreflightParser();
	
	public PDFAnalyser(Config conf) {
	}

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(ArchiveRecordHeader header, InputStream tikainput,
			SolrRecord solr) {
		Metadata metadata = new Metadata();
			metadata.set( Metadata.RESOURCE_NAME_KEY, header.getUrl() );
			ParseRunner parser = new ParseRunner( app, tikainput, metadata, solr );
			Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
			try {
				thread.start();
				thread.join( 30000L );
				thread.interrupt();
			} catch( Exception e ) {
				log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
				solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing with Apache Preflight: " + e.getMessage() );
			}
			
			String isValid = metadata.get( ApachePreflightParser.PDF_PREFLIGHT_VALID );
			solr.addField( SolrFields.PDFA_IS_VALID, isValid );
			String[] errors = metadata.getValues( ApachePreflightParser.PDF_PREFLIGHT_ERRORS );
			if( errors != null ) {
				for( String error : errors ) {
					solr.addField( SolrFields.PDFA_ERRORS, error );
				}
			}
	}

}
