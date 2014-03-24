/**
 * 
 */
package uk.bl.wa.analyser.payload;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.tika.parser.imagefeatures.ColourExtractor;
import uk.bl.wa.tika.parser.imagefeatures.FaceDetectionParser;
import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;

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
	FaceDetectionParser fdp = new FaceDetectionParser();
	
	/** */
	private boolean extractDominantColours = true;
	/** */
	ColourExtractor cep = new ColourExtractor();
	
	public ImageAnalyser(Config conf) {
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
		} catch (IOException e1) {
			e1.printStackTrace();
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
			solr.addField(SolrFields.IMAGE_COLOURS, metadata.get( FaceDetectionParser.DOM_COL));
		}
	}

}
