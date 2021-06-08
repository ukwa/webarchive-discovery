/**
 * 
 */
package uk.bl.wa.annotation;

import java.io.FileNotFoundException;
import java.io.FileReader;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.archive.url.UsableURIFactory;
import org.archive.util.SurtPrefixSet;
import org.jdom.JDOMException;

import uk.bl.wa.indexer.WARCIndexer;
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
    private static Logger LOG = LoggerFactory.getLogger(Annotator.class );
    
    private Annotations annotations;

    private SurtPrefixSet openAccessSurts = null;


    /**
     * Factory method to pull annotations from ACT.
     * 
     * @throws IOException
     * @throws JDOMException
     */
    public static Annotator annotationsFromAct() throws IOException,
            JDOMException {
        AnnotationsFromAct act = new AnnotationsFromAct();
        return new Annotator(act.getAnnotations(), null);
    }

    /**
     * 
     * @param surtPrefixFile
     * @return
     * @throws FileNotFoundException
     */
    public static SurtPrefixSet loadSurtPrefix(String surtPrefixFile)
            throws FileNotFoundException {
        SurtPrefixSet surtPrefix = new SurtPrefixSet();
        FileReader fileReader = new FileReader(surtPrefixFile);
        surtPrefix.importFrom(fileReader);
        return surtPrefix;
    }

    /**
     * 
     * @param annotations
     * @param oaSurts
     */
    public Annotator(Annotations annotations, SurtPrefixSet oaSurts) {
        this.annotations = annotations;
        this.openAccessSurts = oaSurts;
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
        LOG.debug("Updating collections for "
                + solr.getField(SolrFields.SOLR_URL));

        // Trac #2243; This should only happen if the record's timestamp is
        // within the range set by the Collection.
        // So get all the dates:
        // Get all the dates:
        Set<String> crawl_dates = new HashSet<String>();
        crawl_dates.add((String) solr.getField(SolrFields.CRAWL_DATE)
                .getValue());
        if (solr.getField(SolrFields.CRAWL_DATES) != null) {
            for (Object d : solr.getField(SolrFields.CRAWL_DATES).getValues()) {
                @SuppressWarnings("unchecked")
                HashMap<String, String> dhm = (HashMap<String, String>) d;
                crawl_dates.addAll(dhm.values());
            }
        }

        // "Just this URL"
        // String normd = canon.urlStringToKey(uri.toString());
        String normd = uri.toString();
        LOG.debug("Comparing with " + normd);
        if (this.annotations.getCollections().containsKey("resource")) {
            if (this.annotations.getCollections().get("resource").keySet()
                    .contains(normd)) {
                LOG.debug("Applying resource-level annotations...");
                updateCollections(this.annotations.getCollections()
                        .get("resource").get(normd), solr, crawl_dates);
            }
        }
        // "All URLs that start like this".
        if (this.annotations.getCollections().containsKey("root")) {
            if (this.annotations.getCollections().get("root").keySet()
                    .contains(normd)) {
                LOG.debug("Applying root-level annotations...");
                updateCollections(this.annotations.getCollections().get("root")
                        .get(normd), solr, crawl_dates);
            }
        }
        // "All URLs that match match this host or any subdomains".
        if (this.annotations.getCollections().containsKey("subdomains")) {
            String host;
            String domain = uri.getHost().replaceAll("^www\\.", "");
            HashMap<String, UriCollection> subdomains = this.annotations
                    .getCollections().get("subdomains");
            for (String key : subdomains.keySet()) {
                LOG.debug("Applying subdomain annotations for: " + key);
                host = URI.create(key).getHost();
                if (host == null) {
                    host = key;
                }
                if (host.equals(domain) || host.endsWith("." + domain)) {
                    updateCollections(subdomains.get(key), solr, crawl_dates);
                }
            }
        }
        // "All source_file that match this source_file_matches"
        if (this.annotations.getCollections()
                .containsKey("source_file_matches")) {
            Pattern pattern;
            Matcher matcher;
            String sourceFile = (String) solr.getField(SolrFields.SOURCE_FILE)
                    .getValue();
            HashMap<String, UriCollection> sourceFileMatches = this.annotations
                    .getCollections().get("source_file_matches");
            for (String key : sourceFileMatches.keySet()) {
                LOG.debug(
                        "Applying source_file_matches annotations for: " + key);
                pattern = Pattern.compile(key);
                matcher = pattern.matcher(sourceFile);
                while (matcher.find()) {
                    updateCollections(sourceFileMatches.get(key), solr,
                            crawl_dates);
                }
            }
        }

        // Some debugging info:
        /*
         * for (String scope : this.annotations.getCollections().keySet()) {
         * System.err.println("Scope " + scope); System.err.println("GET " +
         * this.annotations.getCollections() .get(scope).get(uri.toString())); }
         */

        // Also use the prefix-based whitelist to note Open Access records:
        if (this.openAccessSurts != null) {
            LOG.debug("Attempting to apply " + this.openAccessSurts.size()
                    + " OA Surts to " + uri);
            String surt = SurtPrefixSet
                    .getCandidateSurt(
                            UsableURIFactory.getInstance(uri.toString()));
            if (this.openAccessSurts.containsPrefixOf(surt)) {
                setUpdateField(solr, SolrFields.ACCESS_TERMS, "OA");
            } else {
                setUpdateField(solr, SolrFields.ACCESS_TERMS, "RRO");
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
            SolrInputDocument solr, Set<String> crawl_dates) {

        // Loop over all the dates:
        for (String dateString : crawl_dates) {
            Date date;
            try {
                date = WARCIndexer.formatter.parse(dateString);
            } catch (ParseException e) {
                LOG.error("Could not parse " + dateString);
                continue;
            }

            LOG.debug("Using collection: " + collection);
            // Update the single, main collection
            if (collection.collection != null
                    && collection.collection.length() > 0) {
                if (this.annotations.getCollectionDateRanges().containsKey(
                        collection.collection)
                        && this.annotations.getCollectionDateRanges()
                                .get(collection.collection)
                                .isInDateRange(date)) {
                    setUpdateField(solr, SolrFields.SOLR_COLLECTION,
                            collection.collection);
                    LOG.debug("Added collection " + collection.collection
                            + " to "
                            + solr.getField(SolrFields.SOLR_URL));
                }
            }
            // Iterate over the hierarchical collections
            if (collection.collections != null
                    && collection.collections.length > 0) {
                for (String col : collection.collections) {
                    LOG.debug("Considering adding collection '" + col + "' to "
                            + solr.getField(SolrFields.SOLR_URL));
                    if (this.annotations.getCollectionDateRanges().containsKey(
                            col)
                            && this.annotations.getCollectionDateRanges()
                                    .get(col).isInDateRange(date)) {
                        setUpdateField(solr, SolrFields.SOLR_COLLECTIONS, col);
                        LOG.debug("Added collection '" + col + "' to "
                                + solr.getField(SolrFields.SOLR_URL));
                    }
                }
            }
            // Iterate over the subjects
            if (collection.subject != null && collection.subject.length > 0) {
                for (String subject : collection.subject) {
                        setUpdateField(solr, SolrFields.SOLR_SUBJECT, subject);
                        LOG.debug("Added collection '" + subject + "' to "
                                + solr.getField(SolrFields.SOLR_URL));
                }
            }
        }
    }

    private static void setUpdateField(SolrInputDocument doc, String field,
            String value) {
        if (doc.getField(field) == null
                || !doc.getField(field).getValues().contains(value)) {
            doc.addField(field, value);
        }
    }


    private static void setSolrUpdateField(SolrInputDocument doc, String field,
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
                    doc.getFieldValues(field)));
        }
        out.println();
    }

    private static void searchAndApplyAnnotations(Annotator anr,
            SolrClient solr, SolrQuery parameters)
            throws SolrServerException,
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
            SurtPrefixSet oaSurts,
            String solrServer) throws SolrServerException, URISyntaxException,
            IOException {
        // Connect to solr:
        SolrClient solr = new HttpSolrClient.Builder(solrServer).build();

        // Set up annotator:
        Annotator anr = new Annotator(ann, oaSurts);

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
        SurtPrefixSet oaSurts = Annotator.loadSurtPrefix(args[1]);
        searchAndApplyAnnotations(ann, oaSurts, args[2]);
    }

}
