/**
 * 
 */
package uk.bl.wa.annotation;

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

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jdom.JDOMException;

import uk.bl.wa.solr.SolrFields;

/**
 * 
 * This is the core annotation class that applies the annotations to a
 * SolrInputDocument.
 * 
 * @author Roger Coram, Andrew Jackson
 * 
 */
public class Annotator {
	private static Log LOG = LogFactory.getLog( Annotator.class );
	
	private Annotations annotations;

	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();
	

	/**
	 * Factory method to pull annotations from ACT.
	 * 
	 * @throws IOException
	 * @throws JDOMException
	 */
	public static Annotator annotationsFromAct() throws IOException,
			JDOMException {
		AnnotationsFromAct act = new AnnotationsFromAct();
		return new Annotator(act.getAnnotations());
	}

	/**
	 * Allow annotations to be defined outside of ACT.
	 * 
	 * @param annotations
	 */
	public Annotator(Annotations annotations) {
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
	public void applyAnnotations(URI uri, SolrInputDocument solr)
			throws URISyntaxException, URIException {
		// "Just this URL"
		// String normd = canon.urlStringToKey(uri.toString());
		String normd = uri.toString();
		LOG.debug("Comparing with " + normd);
		if (this.annotations.getCollections().get("resource").keySet()
				.contains(normd)) {
			LOG.debug("Applying resource-level annotations...");
			updateCollections(this.annotations.getCollections().get("resource")
					.get(normd), solr);
		}
		// "All URLs that start like this".
		String prefix = uri.getScheme() + "://" + uri.getHost();
		if (this.annotations.getCollections().get("root").keySet()
				.contains(prefix)) {
			LOG.debug("Applying root-level annotations...");
			updateCollections(this.annotations.getCollections().get("root")
					.get(prefix), solr);
		}
		// "All URLs that match match this host or any subdomains".
		String host;
		String domain = uri.getHost().replaceAll( "^www\\.", "" );
		HashMap<String, UriCollection> subdomains = this.annotations
				.getCollections().get("subdomains");
		for( String key : subdomains.keySet() ) {
			LOG.debug("Applying subdomain annotations for: " + key);
			host = key;
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
		Date date = (Date) solr.getField(SolrFields.CRAWL_DATE).getValue();

		LOG.debug("Updating collections for "
				+ solr.getField(SolrFields.SOLR_URL));
		LOG.debug("Using collection: " + collection);
		// Update the single, main collection
		if (collection.collection != null && collection.collection.length() > 0) {
			if (this.annotations.getCollectionDateRanges().containsKey(
					collection.collection)
					&& this.annotations.getCollectionDateRanges()
							.get(collection.collection)
							.isInDateRange(date)) {
				setUpdateField(solr, SolrFields.SOLR_COLLECTION,
						collection.collection);
				LOG.debug("Added collection " + collection.collection + " to "
						+ solr.getField(SolrFields.SOLR_URL));
			}
		}
		// Iterate over the hierarchical collections
		if (collection.collections != null
				&& collection.collections.length > 0) {
			for (String col : collection.collections) {
				LOG.debug("Considering adding collection '" + col + "' to "
						+ solr.getField(SolrFields.SOLR_URL));
				if (this.annotations.getCollectionDateRanges().containsKey(col)
						&& this.annotations.getCollectionDateRanges().get(col)
								.isInDateRange(date)) {
					setUpdateField(solr, SolrFields.SOLR_COLLECTIONS, col);
					LOG.debug("Added collection '" + col + "' to "
							+ solr.getField(SolrFields.SOLR_URL));
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
					LOG.debug("Added collection '" + subject + "' to "
							+ solr.getField(SolrFields.SOLR_URL));
				}
			}
		}
	}


	private static void setUpdateField(SolrInputDocument doc, String field,
			String value) {
		Map<String, String> operation = new HashMap<String, String>();
		operation.put("set", value);
		// Check to see if this value is already in:
		boolean newValue = true;
		if (doc.getFieldValues(field) != null) {
			for (Object val : doc.getFieldValues(field)) {
				@SuppressWarnings("unchecked")
				Map<String, String> cmap = (Map<String, String>) val;
				if (cmap.values().contains(value))
					newValue = false;
			}
		}
		// Add it if it is a new value:
		if (newValue) {
			LOG.info("Adding value: " + value + " to field: " + field
					+ " for URI " + doc.getFieldValue(SolrFields.SOLR_URL));
			doc.addField(field, operation);
		} else {
			LOG.debug("Skipping addition of existing field value: " + value
					+ " to field: " + field);
		}
	}

	/**
	 * Pretty-print each solrDocument in the results to stdout
	 * 
	 * @param out
	 * @param doc
	 */
	protected static void prettyPrint(PrintStream out, SolrInputDocument doc) {
		List<String> sortedFieldNames = new ArrayList<String>(
				doc.getFieldNames());
		Collections.sort(sortedFieldNames);
		out.println();
		for (String field : sortedFieldNames) {
			out.println(String.format("\t%s: %s", field,
					doc.getFieldValue(field)));
		}
		out.println();
	}

	private static void searchAndApplyAnnotations(Annotator anr,
			SolrServer solr, SolrQuery parameters) throws SolrServerException,
			URISyntaxException, IOException {
		QueryResponse response = solr.query(parameters);
		SolrDocumentList list = response.getResults();
		for (SolrDocument doc : list) {
			SolrInputDocument solrInDoc = new SolrInputDocument();
			solrInDoc.setField(SolrFields.ID, doc.getFieldValue(SolrFields.ID));
			solrInDoc.setField(SolrFields.CRAWL_DATE,
					doc.getFieldValue(SolrFields.CRAWL_DATE));
			solrInDoc.setField(SolrFields.SOLR_URL,
					doc.getFieldValue(SolrFields.SOLR_URL));
			String uriString = (String) solrInDoc
					.getFieldValue(SolrFields.SOLR_URL);
			URI uri = new URI(uriString);
			// Update all of those records with the applicable
			// categories etc.
			anr.applyAnnotations(uri, solrInDoc);
			solr.add(solrInDoc);
		}
	}

	private static void searchAndApplyAnnotations(Annotations ann,
			String solrServer) throws SolrServerException, URISyntaxException,
			IOException {
		// Connect to solr:
		SolrServer solr = new HttpSolrServer(solrServer);

		// Set up annotator:
		Annotator anr = new Annotator(ann);

		// Loop over URL known to ACT:
		for (String scope : ann.getCollections().keySet()) {
			if ("resource".equals(scope)) {

				// Search for all matching URLs in SOLR:
				for (String uriKey : ann.getCollections().get(scope).keySet()) {
					LOG.info("Looking for URL: " + uriKey);
					SolrQuery parameters = new SolrQuery();
					parameters.set("q",
							"url:" + ClientUtils.escapeQueryChars(uriKey));
					searchAndApplyAnnotations(anr, solr, parameters);
				}

			} else if ("root".equals(scope)) {

				// Search for all matching URLs in SOLR:
				for (String uriKey : ann.getCollections().get(scope).keySet()) {
					LOG.info("Looking for URLs starting with: " + uriKey);
					SolrQuery parameters = new SolrQuery();
					parameters
							.set("q",
									"url:"
											+ ClientUtils
													.escapeQueryChars(uriKey)
											+ "*");
					searchAndApplyAnnotations(anr, solr, parameters);
				}

			} else {
				LOG.warn("Ignoring annotations scoped as: " + scope);
			}
		}

		// And commit:
		solr.commit();

	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws JDOMException
	 * @throws URISyntaxException
	 * @throws SolrServerException
	 */
	public static void main(String[] args) throws IOException, JDOMException,
			URISyntaxException, SolrServerException {
		Annotations ann = Annotations.fromJsonFile(args[0]);
		searchAndApplyAnnotations(ann, args[1]);
	}

}
