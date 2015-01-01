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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.archive.io.ArchiveRecordHeader;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;

import uk.bl.wa.nanite.droid.DroidDetector;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.TikaExtractor;
import uk.gov.nationalarchives.droid.command.action.CommandExecutionException;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;


/**
 * 
 * TODO Entropy, compressibility, fuzzy hashes, etc.
 * 
 * @author anj
 *
 */
public class WARCPayloadAnalysers {
	private static Log log = LogFactory.getLog( WARCPayloadAnalysers.class );
	
	private boolean passUriToFormatTools = false;
	private TikaExtractor tika = null;
	private DroidDetector dd = null;
	private boolean runDroid = true;
	private boolean droidUseBinarySignaturesOnly = false;

	private boolean extractContentFirstBytes = true;
	private int firstBytesLength = 32;

	public HTMLAnalyser html;
	public PDFAnalyser pdf;
	public XMLAnalyser xml;
	public ImageAnalyser image;

	private boolean extractApachePreflightErrors;
	private boolean extractImageFeatures;

	public WARCPayloadAnalysers(Config conf) {
		this.extractContentFirstBytes = conf.getBoolean( "warc.index.extract.content.first_bytes.enabled" );
		this.firstBytesLength = conf.getInt( "warc.index.extract.content.first_bytes.num_bytes" );
		this.runDroid = conf.getBoolean( "warc.index.id.droid.enabled" );
		this.passUriToFormatTools = conf.getBoolean( "warc.index.id.useResourceURI" );
		this.droidUseBinarySignaturesOnly = conf.getBoolean( "warc.index.id.droid.useBinarySignaturesOnly" );

		this.extractApachePreflightErrors = conf.getBoolean( "warc.index.extract.content.extractApachePreflightErrors" );
		this.extractImageFeatures = conf.getBoolean("warc.index.extract.content.images.enabled");
		log.info("Image feature extraction = " + this.extractImageFeatures);
		
		// Attempt to set up Droid:
		try {
			dd = new DroidDetector();
			dd.setBinarySignaturesOnly( droidUseBinarySignaturesOnly );
		} catch( CommandExecutionException e ) {
			e.printStackTrace();
			dd = null;
		}
		
		// Set up Tika:
		tika = new TikaExtractor( conf );
		
		// Set up other extractors:
		html = new HTMLAnalyser(conf);
		if (this.extractApachePreflightErrors) {
			pdf = new PDFAnalyser(conf);
		}
		xml = new XMLAnalyser(conf);
		if (this.extractImageFeatures) {
			image = new ImageAnalyser(conf);
		}
	}
	
	public void analyse(ArchiveRecordHeader header, InputStream tikainput, SolrRecord solr) {
		log.debug("Analysing "+header.getUrl());
		
		// Analyse with tika:
		try {
			if( passUriToFormatTools ) {
				solr = tika.extract( solr, tikainput, header.getUrl() );
			} else {
				solr = tika.extract( solr, tikainput, null );
			}
		} catch( Exception i ) {
			log.error( i + ": " + i.getMessage() + ";tika; " + header.getUrl() + "@" + header.getOffset() );
		}

		// Pull out the first few bytes, to hunt for new format by magic:
		try {
			tikainput.reset();
			byte[] ffb = new byte[ this.firstBytesLength ];
			int read = tikainput.read( ffb );
			if( read >= 4 ) {
				String hexBytes = Hex.encodeHexString( ffb );
				solr.addField( SolrFields.CONTENT_FFB, hexBytes.substring( 0, 2 * 4 ) );
				StringBuilder separatedHexBytes = new StringBuilder();
				for( String hexByte : Splitter.fixedLength( 2 ).split( hexBytes ) ) {
					separatedHexBytes.append( hexByte );
					separatedHexBytes.append( " " );
				}
				if( this.extractContentFirstBytes ) {
					solr.addField( SolrFields.CONTENT_FIRST_BYTES, separatedHexBytes.toString().trim() );
				}
			}
		} catch( Exception i ) {
			log.error( i + ": " + i.getMessage() + ";ffb; " + header.getUrl() + "@" + header.getOffset() );
		}

		// Also run DROID (restricted range):
		if( dd != null && runDroid == true ) {
			try {
				tikainput.reset();
				// Pass the URL in so DROID can fall back on that:
				Metadata metadata = new Metadata();
				if( passUriToFormatTools ) {
					UsableURI uuri = UsableURIFactory.getInstance( header.getUrl() );
					// Droid seems unhappy about spaces in filenames, so hack to avoid:
					String cleanUrl = uuri.getName().replace( " ", "+" );
					metadata.set( Metadata.RESOURCE_NAME_KEY, cleanUrl );
				}
				// Run Droid:
				MediaType mt = dd.detect( tikainput, metadata );
				solr.addField( SolrFields.CONTENT_TYPE_DROID, mt.toString() );
			} catch( Exception i ) {
				// Note that DROID complains about some URLs with an IllegalArgumentException.
				log.error( i + ": " + i.getMessage() + ";dd; " + header.getUrl() + " @" + header.getOffset() );
			}
		}
		
		try {
			tikainput.reset();
			String mime = ( String ) solr.getField( SolrFields.SOLR_CONTENT_TYPE ).getValue();
			if( mime.startsWith( "text" ) || mime.startsWith("application/xhtml+xml") ) {
				html.analyse(header, tikainput, solr);

			} else if( mime.startsWith( "image" ) ) {
				if( this.extractImageFeatures ) {
					image.analyse(header, tikainput, solr);
				}
				
			} else if( mime.startsWith( "application/pdf" ) ) {
				if( extractApachePreflightErrors ) {
					pdf.analyse(header, tikainput, solr);
				}
				
			} else if( mime.startsWith("application/xml") || mime.startsWith("text/xml") ) {
				xml.analyse(header, tikainput, solr);
				
			} else {
				log.debug("No specific additional parser for: "+mime);
			}
		} catch( Exception i ) {
			log.error( i + ": " + i.getMessage() + ";x; " + header.getUrl() + "@" + header.getOffset() );
		}
		
	}
}
