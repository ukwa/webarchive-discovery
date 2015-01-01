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
import org.apache.tika.parser.AbstractParser;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public abstract class AbstractPayloadAnalyser {
	private static Log log = LogFactory.getLog( AbstractPayloadAnalyser.class );

	public abstract void analyse(ArchiveRecordHeader header, InputStream tikainput, SolrRecord solr);

	protected class ParseRunner implements Runnable {
		AbstractParser parser;
		Metadata metadata;
		InputStream input;
		private SolrRecord solr;

		public ParseRunner( AbstractParser parser, InputStream tikainput, Metadata metadata, SolrRecord solr ) {
			this.parser = parser;
			this.metadata = metadata;
			this.input = tikainput;
			this.solr = solr;
		}

		@Override
		public void run() {
			try {
				input.reset();
				parser.parse( input, null, metadata, null );
			} catch( Exception e ) {
				log.error( parser.getClass().getName()+".parse(): " + e.getMessage() );
				// Also record as a Solr PARSE_ERROR
				solr.addParseException("when parsing with "
						+ parser.getClass().getName(), e);
			}
		}
	}
	
}
