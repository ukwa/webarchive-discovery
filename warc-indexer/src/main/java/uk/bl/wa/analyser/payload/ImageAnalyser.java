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

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.tika.parser.imagefeatures.ColourExtractor;
import uk.bl.wa.tika.parser.imagefeatures.FaceDetectionParser;

/**
 * @author anj
 *
 */
public class ImageAnalyser extends AbstractPayloadAnalyser {
	private static Log log = LogFactory.getLog( ImageAnalyser.class );

	/** Maximum file size of images to attempt to parse */
	private static int max_size_bytes = 1024*1024;
	
	/** */
	private boolean extractFaces = true;
	/** */
	FaceDetectionParser fdp;
	
	public ImageAnalyser(Config conf) {
		fdp = new FaceDetectionParser(conf);
	}

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(ArchiveRecordHeader header, InputStream tikainput,
			SolrRecord solr) {
		Metadata metadata = new Metadata();
		// Skip large images:
		if( header.getLength() > max_size_bytes ) {
			return;
		}
		// Get basic info:
		try {
			BufferedImage image = ImageIO.read(tikainput);
			long pixels = image.getWidth()*image.getHeight();
			solr.addField(SolrFields.IMAGE_SIZE, ""+pixels);
		} catch (IOException e) {
			log.error("ImageIO.read failed: "+e);
		}
		
		// Also attempt to extract faces:
		if( this.extractFaces ) {
			ParseRunner parser = new ParseRunner( fdp, tikainput, metadata, solr );
			Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
			try {
				thread.start();
				thread.join( 30000L );
				thread.interrupt();
			} catch( Exception e ) {
				log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
				solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing for faces: " + e.getMessage() );
			}
			// Store faces in SOLR:
			for( String face : metadata.getValues(FaceDetectionParser.FACE_FRAGMENT_ID) ) {
				log.debug("Found a face!");
				solr.addField( SolrFields.IMAGE_FACES, face);
			}
			int faces = metadata.getValues(FaceDetectionParser.FACE_FRAGMENT_ID).length;
			if ( faces > 0 )
				solr.setField( SolrFields.IMAGE_FACES_COUNT, ""+faces );
			// Store colour:
			solr.addField(SolrFields.IMAGE_DOMINANT_COLOUR, metadata.get( FaceDetectionParser.DOM_COL));
		}
	}

}
