/**
 * 
 */
package uk.bl.wa.analyser.payload;

import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.parsers.ApachePreflightParser;
import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;

/**
 * @author anj
 *
 */
public class PDFAnalyser extends AbstractPayloadAnalyser {
	private static Log log = LogFactory.getLog( PDFAnalyser.class );

	/** */
	private ApachePreflightParser app = new ApachePreflightParser();
	private boolean extractApachePreflightErrors = true;
	
	public PDFAnalyser(Config conf) {
		this.extractApachePreflightErrors = conf.getBoolean( "warc.index.extract.content.extractApachePreflightErrors" );
	}

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(ArchiveRecordHeader header, InputStream tikainput,
			SolrRecord solr) {
		Metadata metadata = new Metadata();
		if( extractApachePreflightErrors ) {
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
			solr.addField( "pdf_valid_pdfa_s", isValid );
			String[] errors = metadata.getValues( ApachePreflightParser.PDF_PREFLIGHT_ERRORS );
			if( errors != null ) {
				for( String error : errors ) {
					solr.addField( "pdf_pdfa_errors_ss", error );
				}
			}
		}
	}

}
