/**
 * 
 */
package uk.bl.wa.annotation;

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
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.common.util.Base64;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
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
    
    private String[] crawlFreqs = new String[] { "nevercrawl", "domaincrawl",
            "annual", "sixmonthly", "quarterly", "monthly", "weekly", "daily" };
    private static String WARC_ACT_URL = "http://www.webarchive.org.uk/act/websites/export/daily";
    private static String WARC_COLLECTIONS_URL = "http://www.webarchive.org.uk/act/taxonomy_term.xml?sort=name&direction=ASC&vocabulary=5&limit=500&page=0";
    private static String WARC_COLLECTIONS_URL_JSON = "http://www.webarchive.org.uk/act/taxonomy_term.json?vocabulary=5&limit=500&page=0";
    private static String WARC_SUBJECTS_URL_JSON = "http://www.webarchive.org.uk/act/taxonomy_term.json?vocabulary=2&limit=500&page=0";

    private static Logger LOG = LoggerFactory.getLogger(AnnotationsFromAct.class );
    
    private String cookie;
    private String csrf;

    private static final String COLLECTION_XML = "taxonomy_term";
    private static final String OK_PUBLISH = "1";
    private static final String FIELD_PUBLISH = "field_publish";
    private static final String FIELD_DATES = "field_dates";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_START_DATE = "value";
    private static final String FIELD_END_DATE = "value2";
    
    // Map of all categories and subjects:
    private Map<Integer, JsonNode> cm = new HashMap<Integer, JsonNode>();
    private Map<Integer, JsonNode> sm = new HashMap<Integer, JsonNode>();

    // The annotations being built up from ACT:
    private Annotations ann = new Annotations();

    /**
     * 
     * @throws IOException
     * @throws JDOMException
     */
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

    protected AnnotationsFromAct(String dummy) {
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
        connection.setRequestProperty("Authorization", "Basic "
                + Base64.byteArrayToBase64(credentials.toString().getBytes()));
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
    private String readAct(String url) throws IOException {
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

    /**
     * 
     * @param scope
     * @param urls
     * @param collection
     */
    private void addCollection( String scope, String urls, UriCollection collection ) {
        LOG.debug("Adding " + urls + " to collection " + collection.toString());
        HashMap<String, UriCollection> relevantCollection = ann
                .getCollections().get(scope);
        for( String url : urls.split( "\\s+" ) ) {
            if( scope.equals( "resource" ) ) {
                /*
                 * FIXME try { // Trac #2271: try keying on canonicalized URL.
                 * url = canon.urlStringToKey(url); } catch( URIException u ) {
                 * LOG.warn("Problem parsing URL: " + u.getMessage() + ": " +
                 * url); }
                 */
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

    /**
     * 
     * @return
     */
    public Annotations getAnnotations() {
        return ann;
    }
    
    /**
     * 
     * @param map
     * @param startUrl
     * @throws IOException
     */
    private void getTaxonomyViaJson(Map<Integer, JsonNode> map, String startUrl)
            throws IOException {
        // Get the collections export:
        String nextUrl = startUrl;
        String thisUrl = null;
        // Grab all the pages of collections:
        do {
            // Load the content:
            thisUrl = nextUrl;
            LOG.info("Getting taxnomy export from ACT... " + thisUrl);
            String collectionXml = readAct(thisUrl);

            // Map it to JsonNode tree:
            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = mapper.getJsonFactory().createJsonParser(
                    collectionXml);
            JsonNode root = jp.readValueAsTree();

            // Add to the map of the categories:
            for (JsonNode node : root.get("list")) {
                Integer ci = Integer.parseInt(node.get("tid").textValue());
                map.put(ci, node);
            }
            // Look up the next URL:
            nextUrl = root.path("next").textValue();
            if( nextUrl != null)
                nextUrl = nextUrl.replaceFirst("\\?", "\\.json\\?");
        } while (nextUrl != null);

    }

    /**
     * 
     * @throws JsonParseException
     * @throws IOException
     */
    private void getCollectionsViaJson() throws IOException {
        // Get the subjects taxonomy:
        this.getTaxonomyViaJson(sm, AnnotationsFromAct.WARC_SUBJECTS_URL_JSON);
        // Get the collections taxonomy:
        this.getTaxonomyViaJson(cm,
                AnnotationsFromAct.WARC_COLLECTIONS_URL_JSON);

        // Now patch up the parent-child relationships etc.
        for (JsonNode node : cm.values()) {

            // Get the parent categories:
            List<JsonNode> cats = this.resolveParents(node);

            // Turn that into a string representation:
            String catPath = this.getCatPath(cats);

            // Look to see if the root collection is marked as published:
            Boolean publish = cats.get(0).get("field_publish")
                    .booleanValue();
            if (publish) {
                // LOG.info("Collection Path: " + catPath + " PUBLISHED");
                // Add to list of collections, w/ date ranges:
                String name = catPath;
                String start = null;
                if (cats.get(0).get("field_dates").get("value") != null) {
                    start = cats.get(0).get("field_dates").get("value")
                            .textValue();
                }
                String end = null;
                if (cats.get(0).get("field_dates").get("value2") != null) {
                    end = cats.get(0).get("field_dates").get("value2")
                            .textValue();
                }
                DateRange dateRange = new DateRange(start, end);
                // LOG.info("Adding collection " + name + " with dateRange "
                // + dateRange);
                ann.getCollectionDateRanges().put(name, dateRange);
            } else {
                LOG.debug("Skipping unpublished collection with path: "
                        + catPath);
            }
        }

    }

    /**
     * 
     * @param cats
     * @return
     */
    private String getCatPath(List<JsonNode> cats) {
        // Build up the full path string:
        StringBuilder catPath = new StringBuilder();
        for (int i = 0; i < cats.size(); i++) {
            JsonNode cat = cats.get(i);
            catPath.append(cat.get("name").textValue());
            // Append a separator if this is not the last entry:
            if (i < cats.size() - 1)
                catPath.append("|");
        }
        return catPath.toString();
    }

    /**
     * 
     * @param c
     * @param cats
     */
    private void resolveParents(JsonNode c, List<JsonNode> cats) {
        // Store this item:
        cats.add(0, c);
        // Loop through the parents (although there is only ever one in this
        // dataset):
        for (JsonNode parentRef : c.get("parent")) {
            Integer ci = parentRef.get("id").intValue();
            JsonNode parent = cm.get(ci);
            resolveParents(parent, cats);
        }
    }

    private List<JsonNode> resolveParents(JsonNode c) {
        // Get the parent categories:
        List<JsonNode> cats = new ArrayList<JsonNode>();
        // Find all the parents:
        this.resolveParents(c, cats);
        // And return:
        return cats;
    }

    /**
     * 
     * @throws IOException
     */
    private void getTargetsViaJson() throws IOException {
        String actUrl = "http://www.webarchive.org.uk/act/node.json?type=url";
        int page = 0;
        int max_page = -1;
        do {
            page++;
            LOG.info("Getting page " + page + " of targets export from ACT... "
                    + actUrl);
            String targets = readAct(actUrl);

            ObjectMapper mapper = new ObjectMapper();
            JsonParser jp = mapper.getJsonFactory().createJsonParser(targets);
            JsonNode root = jp.readValueAsTree();

            for (JsonNode node : root.get("list")) {
                String scope = node.get("field_scope").textValue();
                LOG.debug("Got \"" + node.get("title")
                        .textValue()
                        + "\" with scope: " + scope);
                String collectionCategories = null;
                List<String> allCollections = new ArrayList<String>();
                String[] subjects = null;
                // Add on the categories:
                for (JsonNode cat : node.get("field_collection_categories")) {
                    Integer cid = Integer
                            .parseInt(cat.get("id").textValue());
                    JsonNode catd = cm.get(cid);
                    if (catd == null) {
                        LOG.warn("NULL catd for id=" + cid + " from: "
                                + node.asText());
                        continue;
                    }
                    LOG.debug("collectionCategories: "
                            + catd.get("name").textValue());
                    // Get the parent categories:
                    List<JsonNode> catds = this.resolveParents(catd);
                    // Turn that into a string representation:
                    String catPath = this.getCatPath(catds);
                    allCollections.add(catPath);
                    if (collectionCategories == null) {
                        collectionCategories = catds.get(0).get("name")
                                .textValue();
                    }
                }
                // Get the Subject:
                if( node.get("field_subject") != null ) {
                    Integer sid = Integer.parseInt(node.get("field_subject")
                            .get("id").textValue());
                    String subject = sm.get(sid).get("name").textValue();
                    LOG.debug("Found a SUBJECT: "
                            + node.get("field_subject").get("id") + " > "
                            + subject);
                    subjects = new String[] { subject };
                }
                UriCollection uc = new UriCollection(collectionCategories,
                        allCollections.toArray(new String[1]), subjects);
                for (JsonNode url : node.get("field_url")) {
                    LOG.debug("Got " + url.get("url").textValue());
                    // Add to the collection:
                    addCollection(scope, url.get("url").textValue(), uc);
                }
            }

            // Look up the next page URL:
            actUrl = root.path("next").textValue();
            if (actUrl != null)
                actUrl = actUrl.replaceFirst("\\?", "\\.json\\?");
        } while (actUrl != null && (page < max_page || max_page < 0));

        // Summarise the result:
        for (String key : ann.getCollections().keySet()) {
            LOG.info("Processed " + ann.getCollections().get(key).size()
                    + " URIs for collection " + key);
        }
    }

    /**
     * 
     * @param args
     * @throws IOException
     * @throws MalformedURLException
     * @throws JsonParseException
     * @throws JDOMException
     */
    public static void main(String[] args) throws JsonParseException,
            MalformedURLException, IOException, JDOMException {

        // Populate
        LOG.info("Logging into ACT...");
        AnnotationsFromAct act = new AnnotationsFromAct("dummy");
        act.actLogin();

        act.getCollectionsViaJson();

        act.getTargetsViaJson();

        String filename = "annotations.json";

        LOG.info("Writing annotations to: " + filename);
        act.getAnnotations().toJsonFile(filename);
        LOG.info("...done.");
    }

}
