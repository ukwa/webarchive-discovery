/**
 * 
 */
package uk.bl.wa.analyser.payload;

import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AbstractParser;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;

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
				solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing with "+parser.getClass().getName()+": " + e.getMessage() );
			}
		}
	}
	
}
