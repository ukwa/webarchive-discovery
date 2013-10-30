package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecordHeader;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {
	private static final Log LOG = LogFactory.getLog( WARCIndexerMapper.class );

	private WARCIndexer windex;
	private HashMap<String, HashMap<String, UriCollection>> collections;

	private class UriCollection {
		protected String collectionCategories;
		protected String[] allCollections;
		protected String[] subject;

		public UriCollection( String collectionCategories, String allCollections, String subject ) {
			if( collectionCategories != null && collectionCategories.length() > 0 )
				this.collectionCategories = collectionCategories;
			if( allCollections != null && allCollections.length() > 0 )
				this.allCollections = allCollections.split( "\\s*\\|\\s*" );
			if( subject != null && subject.length() > 0 )
				this.subject = subject.split( "\\s*\\|\\s*" );
		}
	}

	public WARCIndexerMapper() {
		collections = new HashMap<String, HashMap<String, UriCollection>>();
		collections.put( "resource", new HashMap<String, UriCollection>() );
		collections.put( "plus1", new HashMap<String, UriCollection>() );
		collections.put( "root", new HashMap<String, UriCollection>() );
		collections.put( "subdomains", new HashMap<String, UriCollection>() );
	}

	@Override
	public void configure( JobConf job ) {
		try {
			// Get config from job property:
			Config config = ConfigFactory.parseString( job.get( WARCIndexerRunner.CONFIG_PROPERTIES ) );
			// If we're reading from ACT, parse the XML output into our collection lookup.
			String xml = job.get( "warc.act.xml" );
			if( xml != null ) {
				parseCollections( xml );
			}
			// Initialise indexer:
			this.windex = new WARCIndexer( config );
		} catch( NoSuchAlgorithmException e ) {
			LOG.error( "ArchiveTikaMapper.configure(): " + e.getMessage() );
		} catch( JDOMException e ) {
			LOG.error( "ArchiveTikaMapper.configure(): " + e.getMessage() );
		} catch( IOException e ) {
			LOG.error( "ArchiveTikaMapper.configure(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, WritableSolrRecord> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();

		if( !header.getHeaderFields().isEmpty() ) {
			SolrRecord solr = windex.extract( key.toString(), value.getRecord() );
			
			if( solr == null ) {
				LOG.debug( "WARCIndexer returned NULL for: " + header.getUrl() );
				return;
			}

			String oKey = null;
			try {
				URI uri = new URI( header.getUrl() );
				processCollectionScopes( uri, solr );
				oKey = uri.getHost();
				if( oKey != null )
					output.collect( new Text( oKey ), new WritableSolrRecord(solr) );
			} catch( Exception e ) {
				LOG.error( e.getClass().getName() + ": " + e.getMessage() + "; " + header.getUrl() + "; " + oKey + "; " + solr );
			}
		}
	}

	/**
	 * Runs through the 4 possible scopes, determining the appropriate part
	 * of the URI to match.
	 * 
	 * @param uri
	 * @param solr
	 * @throws URISyntaxException 
	 */
	private void processCollectionScopes( URI uri, SolrRecord solr ) throws URISyntaxException {
		// "Just this URL".
		if( collections.get( "resource" ).keySet().contains( uri.toString() ) ) {
			updateCollections( collections.get( "resource" ).get( uri.toString() ), solr );
		}
		// "All URLs that start like this".
		String prefix = uri.getScheme() + "://" + uri.getHost();
		if( collections.get( "root" ).keySet().contains( prefix ) ) {
			updateCollections( collections.get( "root" ).get( prefix ), solr );
		}
		// "All URLs that match match this host or any subdomains".
		String domain = uri.getHost().replaceAll( "^www\\.", "" );
		HashMap<String, UriCollection> subdomains = collections.get( "subdomains" );
		Iterator<String> iKeys = subdomains.keySet().iterator();
		while( iKeys.hasNext() ) {
			if( new URI( iKeys.next() ).getHost().endsWith( domain ) ) {
				updateCollections( subdomains.get( iKeys.next() ), solr );
			}
		}
	}

	private void updateCollections( UriCollection collection, SolrRecord solr ) {
		// Update the single, main collection
		solr.addField( SolrFields.SOLR_COLLECTION, collection.collectionCategories );
		// Iterate over the hierarchical collections
		for( String col : collection.allCollections ) {
			solr.addField( SolrFields.SOLR_COLLECTIONS, col );
		}
		// Iterate over the subjects
		for( String subject : collection.subject ) {
			solr.addField( SolrFields.SOLR_SUBJECT, subject );
		}
	}

	@SuppressWarnings( "unchecked" )
	private void parseCollections( String xml ) throws JDOMException, IOException {
		SAXBuilder builder = new SAXBuilder();
		Document document = ( Document ) builder.build( new StringReader( xml ) );
		Element rootNode = document.getRootElement();
		List<Element> list = rootNode.getChildren( "node" );

		Element node = null;
		String urls, collectionCategories, allCollections, subject, scope;
		HashMap<String, UriCollection> relevantCollection;
		for( int i = 0; i < list.size(); i++ ) {
			node = ( Element ) list.get( i );
			urls = node.getChildText( "urls" );
			collectionCategories = node.getChildText( "collectionCategories" );
			allCollections = node.getChildText( "allCollections" );
			subject = node.getChildText( "subject" );
			scope = node.getChildText( "scope" );
			if( collectionCategories != null || allCollections != null || subject != null ) {
				UriCollection collection = new UriCollection( collectionCategories, allCollections, subject );
				for( String url : urls.split( "\\s+" ) ) {
					relevantCollection = collections.get( scope );
					relevantCollection.put( url, collection );
				}
			}
		}
	}
}