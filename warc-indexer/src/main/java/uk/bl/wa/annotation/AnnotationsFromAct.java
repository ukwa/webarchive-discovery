/**
 * 
 */
package uk.bl.wa.annotation;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.google.common.base.Joiner;
import com.sun.syndication.io.impl.Base64;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 
 * This downloads the data from the ACT prototype (based on Drupal) and creates
 * a set of @Annotations from the appropriate taxonomy.
 * 
 * @author Roger Coram, Andrew Jackson
 * 
 */
public class AnnotationsFromAct {
	
	public String[] crawlFreqs = new String[] { "nevercrawl", "domaincrawl",
			"annual", "sixmonthly", "quarterly", "monthly", "weekly", "daily" };
	public static String WARC_ACT_URL = "http://www.webarchive.org.uk/act/websites/export/daily";
	public static String WARC_COLLECTIONS_URL = "http://www.webarchive.org.uk/act/taxonomy_term.xml?sort=name&direction=ASC&vocabulary=5&limit=100&page=0";

	private static Log LOG = LogFactory.getLog( AnnotationsFromAct.class );
	
	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();

	private String cookie;
	private String csrf;

	private static final String COLLECTION_XML = "taxonomy_term";
	private static final String OK_PUBLISH = "1";
	private static final String FIELD_PUBLISH = "field_publish";
	private static final String FIELD_DATES = "field_dates";
	private static final String FIELD_NAME = "name";
	private static final String FIELD_START_DATE = "value";
	private static final String FIELD_END_DATE = "value2";
	
	private Annotations ann = new Annotations();

	public AnnotationsFromAct() throws IOException, JDOMException {
		// Populate
		LOG.info("Logging into ACT...");
		this.actLogin();
		// Get the collections export:
		LOG.info("Getting collections export from ACT...");
		String collectionXml = readAct(AnnotationsFromAct.WARC_COLLECTIONS_URL);
		LOG.info("Parsing collection XML...");
		parseCollectionXml(collectionXml);
		// Get all Targets:
		LOG.info("Getting main export from ACT...");
		String recordXml = readAct(AnnotationsFromAct.WARC_ACT_URL);
		LOG.info("Parsing record XML...");
		parseRecordXml(recordXml);

	}


	/**
	 * Performs login operation to ACT, setting Cookie and CSRF.
	 * @throws IOException
	 */
	private void actLogin() throws IOException {
		Config loginConf = ConfigFactory
				.parseFile(new File("credentials.conf"));
		URL login = new URL( loginConf.getString( "act.login" ) );
		LOG.info("Logging in at " + login);

		HttpURLConnection connection = ( HttpURLConnection ) login.openConnection();
		StringBuilder credentials = new StringBuilder();
		credentials.append( loginConf.getString( "act.username" ) );
		credentials.append( ":" );
		credentials.append( loginConf.getString( "act.password" ) );
		connection.setRequestProperty( "Authorization", "Basic " + Base64.encode( credentials.toString() ) );
		connection.setRequestProperty("Content-Type", "text/plain");

		Scanner scanner;
		if( connection.getResponseCode() != 200 ) {
			scanner = new Scanner( connection.getErrorStream() );
			scanner.useDelimiter( "\\Z" );
			throw new IOException( scanner.next() );
		} else {
			scanner = new Scanner( connection.getInputStream() );
		}
		scanner.useDelimiter( "\\Z" );
		this.csrf = scanner.next();
		this.cookie = connection.getHeaderField( "set-cookie" );
	}

	/**
	 * Read data from ACT to include curator-specified metadata.
	 * @param conf
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	protected String readAct( String url ) throws IOException {
		URL act = new URL( url );
		HttpURLConnection connection = ( HttpURLConnection ) act.openConnection();
		if( this.cookie != null ) {
			connection.setRequestProperty( "Cookie", this.cookie );
			connection.setRequestProperty( "X-CSRF-TOKEN", this.csrf );
		}

		Scanner scanner;
		if( connection.getResponseCode() != 200 ) {
			scanner = new Scanner( connection.getErrorStream() );
			scanner.useDelimiter( "\\Z" );
			throw new IOException( scanner.next() );
		} else {
			scanner = new Scanner( connection.getInputStream() );
		}
		scanner.useDelimiter( "\\Z" );
		return scanner.next();
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
			name = node.getChildText(FIELD_NAME);
			if( publish != null && publish.equals( OK_PUBLISH ) ) {
				start = node.getChild( FIELD_DATES ).getChildText( FIELD_START_DATE );
				end = node.getChild( FIELD_DATES ).getChildText( FIELD_END_DATE );
				dateRange = new DateRange( start, end );
				LOG.info("Adding collection " + name + " with dateRange "
						+ dateRange);
				ann.getCollectionDateRanges().put(name, dateRange);
			} else {
				LOG.info("Skipping collection \"" + name
						+ "\" (not ok to publish)");
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
		Set<String> validCollections = ann.getCollectionDateRanges().keySet();

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
			LOG.info("Looking at scope [" + scope + "] subject [" + subject
					+ "] collectionCategories [" + collectionCategories
					+ "] w/ collections [" + allCollections + "]");
			// As long as one of the fields is populated we have something to do...
			if( collectionCategories != null || allCollections != null || subject != null ) {
				UriCollection collection = filterUriCollection( collectionCategories, allCollections, subject );
				LOG.info("Filtered to " + collection);
				// There should be no scope beyond those created in the Constructor.
				if( collection != null )
					addCollection( scope, urls, collection );
			}
		}
		for (String key : ann.getCollections().keySet()) {
			LOG.info("Processed " + ann.getCollections().get(key).size()
					+ " URIs for collection " + key);
		}
	}

	private void addCollection( String scope, String urls, UriCollection collection ) {
		LOG.info("Adding " + urls + " to collection " + collection);
		HashMap<String, UriCollection> relevantCollection = ann
				.getCollections().get(scope);
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

	public Annotations getAnnotations() {
		return ann;
	}
	
}
