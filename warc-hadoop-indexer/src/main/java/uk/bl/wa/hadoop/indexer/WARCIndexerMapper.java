package uk.bl.wa.hadoop.indexer;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecordHeader;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {
	private static final Log LOG = LogFactory.getLog( WARCIndexerMapper.class );
	private static final String COLLECTION_XML = "taxonomy_term";
	private static final String OK_PUBLISH = "1";
	private static final String FIELD_PUBLISH = "field_publish";
	private static final String FIELD_DATES = "field_dates";
	private static final String FIELD_NAME = "name";
	private static final String FIELD_START_DATE = "value";
	private static final String FIELD_END_DATE = "value2";

	private WARCIndexer windex;
	private HashMap<String, HashMap<String, UriCollection>> collections;
	private HashMap<String, DateRange> collectionDateRanges;
	private boolean processCollections = false;
	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();

	/**
	 * DateRange: holds a start/end date for mapping Collection timeframes.
	 * 
	 * @author rcoram
	 * 
	 */
	private class DateRange {
		protected Date start;
		protected Date end;

		public DateRange( String start, String end ) {
			if( start != null )
				this.start = new Date( Long.parseLong( start ) * 1000L );
			else
				this.start = new Date( 0L );

			if( end != null )
				this.end = new Date( Long.parseLong( end ) * 1000L );
			else
				this.end = new Date( Long.MAX_VALUE );
		}

		public boolean isInDateRange( Date date ) {
			return( date.after( start ) && date.before( end ) );
		}
	}

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
			String recordXml = job.get( "warc.act.xml" );
			String collectionXml = job.get( "warc.act.collections.xml" );
			if( recordXml != null ) {
				processCollections = true;
				LOG.info( "Parsing collection XML..." );
				parseCollectionXml( collectionXml );
				LOG.info( "Parsing record XML..." );
				parseRecordXml( recordXml );
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

			Text oKey = new Text( ( String ) solr.getFieldValue( SolrFields.SOLR_HOST ) );
			try {
				URI uri = new URI( header.getUrl() );
				if( processCollections ) {
					processCollectionScopes( uri, solr );
				}
				WritableSolrRecord wsolr = new WritableSolrRecord( solr );
				output.collect( oKey, wsolr );
			} catch( Exception e ) {
				LOG.error( e.getClass().getName() + ": " + e.getMessage() + "; " + header.getUrl() + "; " + oKey + "; " + solr );
			}
		}
	}

	/**
	 * Runs through the 3 possible scopes, determining the appropriate part
	 * of the URI to match.
	 * 
	 * @param uri
	 * @param solr
	 * @throws URISyntaxException
	 * @throws URIException
	 */
	private void processCollectionScopes( URI uri, SolrRecord solr ) throws URISyntaxException, URIException {
		// "Just this URL".
		if( collections.get( "resource" ).keySet().contains( canon.urlStringToKey( uri.toString() ) ) ) {
			updateCollections( collections.get( "resource" ).get( uri.toString() ), solr );
		}
		// "All URLs that start like this".
		String prefix = uri.getScheme() + "://" + uri.getHost();
		if( collections.get( "root" ).keySet().contains( prefix ) ) {
			updateCollections( collections.get( "root" ).get( prefix ), solr );
		}
		// "All URLs that match match this host or any subdomains".
		String host;
		String domain = uri.getHost().replaceAll( "^www\\.", "" );
		HashMap<String, UriCollection> subdomains = collections.get( "subdomains" );
		for( String key : subdomains.keySet() ) {
			host = new URI( key ).getHost();
			if( host.equals( domain ) || host.endsWith( "." + domain ) ) {
				updateCollections( subdomains.get( key ), solr );
			}
		}
	}

	/**
	 * Updates a given SolrRecord with collections details from a UriCollection.
	 * 
	 * @param collection
	 * @param solr
	 */
	private void updateCollections( UriCollection collection, SolrRecord solr ) {
		// Trac #2243; This should only happen if the record's timestamp is
		// within the range set by the Collection.
		Date date = WARCIndexer.getWaybackDate( ( String ) solr.getField( SolrFields.CRAWL_DATE ).getValue() );

		LOG.info( "Updating collections for " + solr.getField( SolrFields.SOLR_URL ) );
		// Update the single, main collection
		if( collection.collectionCategories != null && collection.collectionCategories.length() > 0 ) {
			if( collectionDateRanges.containsKey( collection.collectionCategories ) && collectionDateRanges.get( collection.collectionCategories ).isInDateRange( date ) ) {
				solr.addField( SolrFields.SOLR_COLLECTION, collection.collectionCategories );
				LOG.info( "Added collection " + collection.collectionCategories + " to " + solr.getField( SolrFields.SOLR_URL ) );
			}
		}
		// Iterate over the hierarchical collections
		if( collection.allCollections != null && collection.allCollections.length > 0 ) {
			for( String col : collection.allCollections ) {
				if( collectionDateRanges.containsKey( col ) && collectionDateRanges.get( col ).isInDateRange( date ) ) {
					solr.addField( SolrFields.SOLR_COLLECTIONS, col );
					LOG.info( "Added collection '" + col + "' to " + solr.getField( SolrFields.SOLR_URL ) );
				}
			}
		}
		// Iterate over the subjects
		if( collection.subject != null && collection.subject.length > 0 ) {
			for( String subject : collection.subject ) {
				if( collectionDateRanges.containsKey( subject ) && collectionDateRanges.get( subject ).isInDateRange( date ) ) {
					solr.addField( SolrFields.SOLR_SUBJECT, subject );
					LOG.info( "Added collection '" + subject + "' to " + solr.getField( SolrFields.SOLR_URL ) );
				}
			}
		}
	}

	/**
	 * Parses XML from ACT, mapping collection names to date ranges.
	 * 
	 * @throws IOException
	 * @throws JDOMException
	 * 
	 */
	@SuppressWarnings( "unchecked" )
	private void parseCollectionXml( String xml ) throws JDOMException, IOException {
		SAXBuilder builder = new SAXBuilder();
		Document document = ( Document ) builder.build( new StringReader( xml ) );
		Element rootNode = document.getRootElement();
		List<Element> list = rootNode.getChildren( COLLECTION_XML );

		Element node = null;
		DateRange dateRange;
		String name, start, end, publish;
		for( int i = 0; i < list.size(); i++ ) {
			node = ( Element ) list.get( i );
			publish = node.getChildText( FIELD_PUBLISH );
			if( publish != null && publish.equals( OK_PUBLISH ) ) {
				name = node.getChildText( FIELD_NAME );
				start = node.getChild( FIELD_DATES ).getChildText( FIELD_START_DATE );
				end = node.getChild( FIELD_DATES ).getChildText( FIELD_END_DATE );
				dateRange = new DateRange( start, end );
				collectionDateRanges.put( name, dateRange );
			}
		}
	}

	/**
	 * Removes inactive Collections before optionally creating a UriCollection.
	 * 
	 * @param collectionCategories
	 * @param allCollections
	 * @param subject
	 * @return
	 */
	private UriCollection filterUriCollection( String collectionCategories, String allCollections, String subject ) {
		UriCollection output = null;
		Set<String> validCollections = collectionDateRanges.keySet();

		if( collectionCategories != null && !validCollections.contains( collectionCategories ) )
			collectionCategories = null;

		ArrayList<String> valid = new ArrayList<String>();
		if( allCollections != null ) {
			for( String a : allCollections.split( "|" ) ) {
				if( validCollections.contains( a ) )
					valid.add( a );
			}
			if( valid.size() == 0 ) {
				allCollections = null;
			} else {
				allCollections = Joiner.on( "|" ).join( valid );
			}
		}

		valid.clear();
		if( subject != null ) {
			for( String s : subject.split( "|" ) ) {
				if( validCollections.contains( s ) )
					valid.add( s );
			}
			if( valid.size() == 0 ) {
				subject = null;
			} else {
				subject = Joiner.on( "|" ).join( valid );
			}
		}

		if( collectionCategories != null && allCollections != null && subject != null )
			output = new UriCollection( collectionCategories, allCollections, subject );

		return output;
	}

	/**
	 * Parses XML output from ACT into a lookup, mapping URLs to collections.
	 * 
	 * @param xml
	 * @throws JDOMException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@SuppressWarnings( "unchecked" )
	private void parseRecordXml( String xml ) throws JDOMException, IOException {
		SAXBuilder builder = new SAXBuilder();
		Document document = ( Document ) builder.build( new StringReader( xml ) );
		Element rootNode = document.getRootElement();
		List<Element> list = rootNode.getChildren( "node" );

		Element node = null;
		String urls, collectionCategories, allCollections, subject, scope;
		for( int i = 0; i < list.size(); i++ ) {
			node = ( Element ) list.get( i );
			urls = node.getChildText( "urls" );
			collectionCategories = node.getChildText( "collectionCategories" );
			// Trac #2271: Erroneous data in ACT might contain pipe-separated text.
			if( collectionCategories != null && collectionCategories.indexOf( "|" ) != -1 ) {
				collectionCategories = collectionCategories.split( "|" )[ 0 ];
			}
			allCollections = node.getChildText( "allCollections" );
			subject = node.getChildText( "subject" );
			scope = node.getChildText( "scope" );
			// As long as one of the fields is populated we have something to do...
			if( collectionCategories != null || allCollections != null || subject != null ) {
				UriCollection collection = filterUriCollection( collectionCategories, allCollections, subject );
				// There should be no scope beyond those created in the Constructor.
				if( collection != null )
					addCollection( scope, urls, collection );
			}
		}
		for( String key : collections.keySet() ) {
			LOG.info( "Processed " + collections.get( key ).size() + " URIs for collection " + key );
		}
	}

	private void addCollection( String scope, String urls, UriCollection collection ) {
		HashMap<String, UriCollection> relevantCollection = collections.get( scope );
		for( String url : urls.split( "\\s+" ) ) {
			if( scope.equals( "resource" ) ) {
				try {
					// Trac #2271: try keying on canonicalized URL.
					url = canon.urlStringToKey( url );
				} catch( URIException u ) {
					LOG.warn( u.getMessage() + ": " + url );
				}
				relevantCollection.put( url, collection );
			} else {
				URI uri;
				try {
					uri = new URI( url );
				} catch( URISyntaxException e ) {
					LOG.warn( e.getMessage() );
					continue;
				}
				if( scope.equals( "root" ) ) {
					String prefix = uri.getScheme() + "://" + uri.getHost();
					relevantCollection.put( prefix, collection );
				}
				if( scope.equals( "subdomains" ) ) {
					String host = uri.getHost();
					relevantCollection.put( host, collection );
				}
			}
		}
	}
}
