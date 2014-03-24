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

import uk.bl.wa.parsers.XMLRootNamespaceParser;
import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;

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
				solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing for XML Root Namespace: " + e.getMessage() );
			}
			solr.addField( "xml_ns_root_s", metadata.get(XMLRootNamespaceParser.XML_ROOT_NS));
		}
	}

}
