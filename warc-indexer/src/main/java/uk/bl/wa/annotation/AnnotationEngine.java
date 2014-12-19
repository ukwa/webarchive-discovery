/**
 * 
 */
package uk.bl.wa.annotation;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jdom.JDOMException;

import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;

/**
 * 
 * 
 * @author Roger Coram, Andrew Jackson
 * 
 */
public class AnnotationEngine {
	private static Log LOG = LogFactory.getLog( AnnotationEngine.class );
	
	private ActAnnotationsClient act;

	private Annotations annotations;

	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();
	

	/**
	 * By default we pull annotations from ACT.
	 * 
	 * @throws IOException
	 * @throws JDOMException
	 */
	public AnnotationEngine() throws IOException, JDOMException {
		act = new ActAnnotationsClient();
		this.annotations = act.getAnnotations();

	}

	/**
	 * Allow annotations to be defined outside of ACT.
	 * 
	 * @param annotations
	 */
	public AnnotationEngine(Annotations annotations) {
		this.annotations = annotations;
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
	private void processCollectionScopes(URI uri, SolrInputDocument solr)
			throws URISyntaxException, URIException {
		// "Just this URL".
		if (this.annotations.getCollections().get("resource").keySet()
				.contains(canon.urlStringToKey(uri.toString()))) {
			updateCollections(this.annotations.getCollections().get("resource")
					.get(uri.toString()), solr);
		}
		// "All URLs that start like this".
		String prefix = uri.getScheme() + "://" + uri.getHost();
		if (this.annotations.getCollections().get("root").keySet()
				.contains(prefix)) {
			updateCollections(this.annotations.getCollections().get("root")
					.get(prefix), solr);
		}
		// "All URLs that match match this host or any subdomains".
		String host;
		String domain = uri.getHost().replaceAll( "^www\\.", "" );
		HashMap<String, UriCollection> subdomains = this.annotations
				.getCollections().get("subdomains");
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
	private void updateCollections(UriCollection collection,
			SolrInputDocument solr) {
		// Trac #2243; This should only happen if the record's timestamp is
		// within the range set by the Collection.
		Date date = WARCIndexer.getWaybackDate( ( String ) solr.getField( SolrFields.CRAWL_DATE ).getValue() );

		LOG.info( "Updating collections for " + solr.getField( SolrFields.SOLR_URL ) );
		// Update the single, main collection
		if( collection.collectionCategories != null && collection.collectionCategories.length() > 0 ) {
			if (this.annotations.getCollectionDateRanges().containsKey(
					collection.collectionCategories)
					&& this.annotations.getCollectionDateRanges()
							.get(collection.collectionCategories)
							.isInDateRange(date)) {
				setUpdateField(solr, SolrFields.SOLR_COLLECTION,
						collection.collectionCategories);
				LOG.info( "Added collection " + collection.collectionCategories + " to " + solr.getField( SolrFields.SOLR_URL ) );
			}
		}
		// Iterate over the hierarchical collections
		if( collection.allCollections != null && collection.allCollections.length > 0 ) {
			for( String col : collection.allCollections ) {
				if (this.annotations.getCollectionDateRanges().containsKey(col)
						&& this.annotations.getCollectionDateRanges().get(col)
								.isInDateRange(date)) {
					setUpdateField(solr, SolrFields.SOLR_COLLECTIONS, col);
					LOG.info( "Added collection '" + col + "' to " + solr.getField( SolrFields.SOLR_URL ) );
				}
			}
		}
		// Iterate over the subjects
		if( collection.subject != null && collection.subject.length > 0 ) {
			for( String subject : collection.subject ) {
				if (this.annotations.getCollectionDateRanges().containsKey(
						subject)
						&& this.annotations.getCollectionDateRanges()
								.get(subject).isInDateRange(date)) {
					setUpdateField(solr, SolrFields.SOLR_SUBJECT, subject);
					LOG.info( "Added collection '" + subject + "' to " + solr.getField( SolrFields.SOLR_URL ) );
				}
			}
		}
	}


	private static void setUpdateField(SolrInputDocument doc, String field,
			String value) {
		Map<String, String> operation = new HashMap<String, String>();
		operation.put("set", value);
		doc.addField(field, operation);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws JDOMException 
	 * @throws URISyntaxException 
	 */
	public static void main(String[] args) throws IOException, JDOMException, URISyntaxException {
		
		AnnotationEngine ae = new AnnotationEngine();
		
		URI uri = URI.create("http://news.bbc.co.uk/");
		SolrInputDocument solr = new SolrInputDocument();
		// Needs ID CrawlDate
		// SolrFields.CRAWL_DATE;
		// SolrFields.ID;

		// Uses SOLR_URL for logging only:
		// SolrFields.SOLR_URL;

		ae.processCollectionScopes( uri, solr );
		
		// Loop over URL known to ACT:

		// Search for all matching URLs in SOLR:

		// Update all of those records with the applicable categories etc.
		
	}

}
