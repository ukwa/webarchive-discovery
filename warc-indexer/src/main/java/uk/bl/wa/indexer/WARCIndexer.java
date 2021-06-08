package uk.bl.wa.indexer;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_ID;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_IP;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.ProtocolException;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpHeaders;
import org.apache.log4j.PropertyConfigurator;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.url.SURT;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;
import org.archive.util.ArchiveUtils;
import org.archive.util.SurtPrefixSet;
import org.archive.wayback.accesscontrol.staticmap.StaticMapExclusionFilterFactory;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.filters.ExclusionFilter;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import uk.bl.wa.analyser.TextAnalysers;
import uk.bl.wa.analyser.WARCPayloadAnalysers;
import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.annotation.Annotator;
import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.util.InputStreamUtils;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;

/**
 * 
 * Core indexer class that takes a web archive record and generates a Solr record.
 * 
 * TODO Currently a rather crude, monolithic code structure. Should pull the different metadata generation logic out into separate classes or at least methods.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class WARCIndexer {
    private static Logger log = LoggerFactory.getLogger(WARCIndexer.class );

    private List<String> url_excludes;
    private List<String> protocol_includes;
    private List<String> response_includes;
    private List<String> record_type_includes;

    private MessageDigest md5 = null;

    /** */
    private boolean extractText;
    private boolean storeText;

    /** Wayback-style URI filtering: */
    private StaticMapExclusionFilterFactory smef = null;

    /** Hook to the solr server: */
    private boolean checkSolrForDuplicates = false;
    private SolrWebServer solrServer = null;
    
    /** Payload Analysers */
    private long inMemoryThreshold;
    private long onDiskThreshold;
    private final InputStreamUtils.HASH_STAGE hashStage;
    private WARCPayloadAnalysers wpa;
    
    /** Text Analysers */
    private TextAnalysers txa;

    /** Annotations */
    private Annotator ant = null;

    // Paired with HtmlFeatureParsers links-extractor
    private final boolean addNormalisedURL;

    // Also canonicalise the HOST field (e.g. drop "www.")
    public static final boolean CANONICALISE_HOST = true;

    private final SolrRecordFactory solrFactory;
    private final java.time.format.DateTimeFormatter WAYBACK_DATETIME =
            java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.of("UTC"));
//    private final DateTimeFormatter WAYBACK_DATETIME = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    /* ------------------------------------------------------------ */

    /**
     * Default constructor, with empty configuration.
     */
    public WARCIndexer() throws NoSuchAlgorithmException {
        this( ConfigFactory.parseString( ConfigFactory.load().root().render( ConfigRenderOptions.concise() ) ) );
    }

    /**
     * Preferred constructor, allows passing in configuration from execution environment.
     */
    public WARCIndexer( Config conf ) throws NoSuchAlgorithmException {
        log.info("Initialising WARCIndexer...");
        try {
            Properties props = new Properties();
            if (getClass().getResource("/log4j-override.properties") != null) {
                try (Reader resourceAsStream = new InputStreamReader(getClass().getResourceAsStream(
                        "/log4j-override.properties"), StandardCharsets.UTF_8)) {
                    props.load(resourceAsStream);
                }
            }
            PropertyConfigurator.configure(props);
        } catch (IOException e1) {
            log.error("Failed to load log4j config from properties file.");
        }
        solrFactory = SolrRecordFactory.createFactory(conf);
        // Optional configurations:
        this.extractText = conf.getBoolean( "warc.index.extract.content.text" );
        log.info("Extract text = " + extractText);
        this.storeText = conf
                .getBoolean("warc.index.extract.content.text_stored");
        log.info("Store text = " + storeText);
        addNormalisedURL = conf.hasPath(HtmlFeatureParser.CONF_LINKS_NORMALISE) ?
                conf.getBoolean(HtmlFeatureParser.CONF_LINKS_NORMALISE) :
                HtmlFeatureParser.DEFAULT_LINKS_NORMALISE;
        this.checkSolrForDuplicates = conf.getBoolean("warc.solr.check_solr_for_duplicates");
        if (this.checkSolrForDuplicates == true) {
            log.warn(
                    "Checking Solr for duplicates is not implemented at present!");
        }
        // URLs to exclude:
        this.url_excludes = conf.getStringList( "warc.index.extract.url_exclude" );
        // Protocols to include:
        this.protocol_includes = conf.getStringList( "warc.index.extract.protocol_include" );
        // Response codes to include:
        this.response_includes = conf.getStringList( "warc.index.extract.response_include" );
        // Record types to include:
        this.record_type_includes = conf.getStringList( "warc.index.extract.record_type_include" );

        // URL Filtering options:
        if( conf.getBoolean( "warc.index.exclusions.enabled" ) ) {
            smef = new StaticMapExclusionFilterFactory();
            smef.setFile( conf.getString( "warc.index.exclusions.file" ) );
            smef.setCheckInterval( conf.getInt( "warc.index.exclusions.check_interval" ) );
            try {
                smef.init();
            } catch( IOException e ) {
                log.error( "Failed to load exclusions file." );
                throw new RuntimeException( "StaticMapExclusionFilterFactory failed with IOException when loading " + smef.getFile() );
            }
        }

        // Instanciate required helpers:
        md5 = MessageDigest.getInstance( "MD5" );
        
        // Also hook up to Solr server for queries:
        if( this.checkSolrForDuplicates ) {
            log.info("Initialisating connection to Solr...");
            solrServer = new SolrWebServer(conf);
        }
        
        // Set up hash-cache properties:
        this.inMemoryThreshold = conf.getBytes( "warc.index.extract.inMemoryThreshold" );
        this.onDiskThreshold = conf.getBytes( "warc.index.extract.onDiskThreshold" );
        this.hashStage = conf.hasPath("warc.index.extract.hashStage") ?
                InputStreamUtils.HASH_STAGE.valueOf(conf.getString("warc.index.extract.hashStage")) :
                InputStreamUtils.DEFAULT_HASH_STAGE;
        log.info("Hashing & Caching thresholds are: < "+this.inMemoryThreshold+" in memory, < "+
                 this.onDiskThreshold+" on disk. HashStage = " + this.hashStage);
        
        // Set up analysers
        log.info("Setting up analysers...");
        this.wpa = new WARCPayloadAnalysers(conf);
        this.txa = new TextAnalysers(conf);


        // Set up annotator
        if (conf.hasPath("warc.index.extract.content.annotations.enabled") && conf.getBoolean("warc.index.extract.content.annotations.enabled")) {
            String annotationsFile = conf.getString("warc.index.extract.content.annotations.file");
            String openAccessSurtsFile = conf.getString("warc.index.extract.content.annotations.surt_prefix_file");
            try {
                Annotations ann = Annotations.fromJsonFile(annotationsFile);
                SurtPrefixSet oaSurts = Annotator.loadSurtPrefix(openAccessSurtsFile);
                this.ant = new Annotator(ann, oaSurts);
            } catch (IOException e) {
                log.error("Failed to load annotations files.");
                throw new RuntimeException("Annotations failed with IOException when loading files " + annotationsFile + ", " + openAccessSurtsFile);
            }
        }

        // We want stats for the 20 resource types that we spend the most time processing
        Instrument.createSortedStat("WARCIndexer#content_types", Instrument.SORT.time, 20);

        // Log so it's clear this completed ok:
        log.info("Initialisation of WARCIndexer complete.");
    }

    /**
     * 
     * @param ann
     */
    public void setAnnotations(Annotations ann, SurtPrefixSet openAccessSurts) {
        this.ant = new Annotator(ann, openAccessSurts);
    }

    /**
     * @return the checkSolrForDuplicates
     */
    public boolean isCheckSolrForDuplicates() {
        return checkSolrForDuplicates;
    }

    /**
     * @param checkSolrForDuplicates the checkSolrForDuplicates to set
     */
    public void setCheckSolrForDuplicates(boolean checkSolrForDuplicates) {
        this.checkSolrForDuplicates = checkSolrForDuplicates;
    }

    /**
     * This extracts metadata and text from the ArchiveRecord and creates a suitable SolrRecord.
     * 
     * @param archiveName
     * @param record
     * @return
     * @throws IOException
     */
    public SolrRecord extract( String archiveName, ArchiveRecord record ) throws IOException {
        return this.extract( archiveName, record, this.extractText );
    }

    /**
     * This extracts metadata from the ArchiveRecord and creates a suitable SolrRecord.
     * Removes the text field if flag set.
     * 
     * @param archiveName
     * @param record
     * @param isTextIncluded
     * @return
     * @throws IOException
     */
    public SolrRecord extract( String archiveName, ArchiveRecord record, boolean isTextIncluded ) throws IOException {      
      final long start = System.nanoTime();
        final ArchiveRecordHeader header = record.getHeader();
        final SolrRecord solr = solrFactory.createRecord(archiveName, header);
        
        if( !header.getHeaderFields().isEmpty() ) {
            if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) ) {
                log.debug("Looking at " + header.getHeaderValue(HEADER_KEY_TYPE));

                if( !checkRecordType( ( String ) header.getHeaderValue( HEADER_KEY_TYPE ) ) ) {
                    return null;
                }
                processWARCHeader(header, solr);
            } else {
                processARCHeader(solr);
            }

            if( header.getUrl() == null )
                return null;

            // Get the URL:
            String targetUrl = Normalisation.sanitiseWARCHeaderValue(header.getUrl());

            // Strip down very long URLs to avoid
            // "org.apache.commons.httpclient.URIException: Created (escaped)
            // uuri > 2083"
            // Trac #2271: replace string-splitting with URI-based methods.
            if (targetUrl.length() > 2000)
                targetUrl = targetUrl.substring(0, 2000);

            log.debug("Current heap usage: "
                    + FileUtils.byteCountToDisplaySize(Runtime.getRuntime()
                            .totalMemory()));
            log.debug("Processing " + targetUrl + " from " + archiveName);

            // Check the filters:
            if(!this.checkProtocol(targetUrl) || !this.checkUrl(targetUrl) || !this.checkExclusionFilter(targetUrl)) {
                return null;
            }
            setUserFields(solr, archiveName);

            // Add URL-based fields:
            final URI saneURI = parseURL(solr, targetUrl);

            // --- Basic headers ---
            processEnvelopeHeader(solr, header, targetUrl);

            Instrument.timeRel("WARCIndexer.extract#total",
                               "WARCIndexer.extract#archeaders", start);

            // -----------------------------------------------------
            // Now consume record and HTTP headers (only)
            // -----------------------------------------------------

            // Only parse HTTP headers for HTTP URIs
            HTTPHeader httpHeader = null;
            if( targetUrl.startsWith( "http" ) ) {
                // TODO: Consider extracting & temporarily keeping all HTTP-header for later hinting on compression etc.
                if( record instanceof WARCRecord ) {
                    httpHeader = this.processWARCHTTPHeaders(record, header, targetUrl, solr);
                } else if( record instanceof ARCRecord ) {
                    ARCRecord arcr = ( ARCRecord ) record;
                    httpHeader = new HTTPHeader();
                    httpHeader.setHttpStatus(Integer.toString(arcr.getStatusCode()));
                    httpHeader.addAll(arcr.getHttpHeaders());

                    this.processHTTPHeaders(solr, httpHeader, targetUrl);
                    arcr.skipHttpHeader();
                } else {
                    log.error( "FAIL! Unsupported archive record type " + record.getClass().getCanonicalName() );
                    return solr;
                }

                solr.setField(SolrFields.SOLR_STATUS_CODE, httpHeader.getHttpStatus());

                // Skip recording non-content URLs (i.e. 2xx responses only please):
                if(!checkResponseCode(httpHeader.getHttpStatus())) {
                    log.debug( "Skipping this record based on status code " + httpHeader.getHttpStatus() + ": " + targetUrl );
                    return null;
                }
            } else {
                log.info("Skipping header parsing as URL does not start with 'http'");
            }
            
            // -----------------------------------------------------
            // Headers have been processed, payload ready to cache:
            // -----------------------------------------------------
            // Update the content_length based on what's available:
            final long content_length = record.available();

            // Record the length:
            solr.setField(SolrFields.CONTENT_LENGTH, ""+content_length);
            
            // Create an appropriately cached version of the payload, to allow analysis.
            final long hashStreamStart = System.nanoTime();
            final InputStreamUtils.HashIS hashIS = InputStreamUtils.cacheDecompressDechunkHash(
                    record, content_length, targetUrl, header, httpHeader,
                    this.inMemoryThreshold, this.onDiskThreshold, this.hashStage);

            // If the hash didn't match, record it:
            if (!hashIS.getHashStream().isHashMatched()) {
                // Facet-friendly:
                solr.addField(SolrFields.PARSE_ERROR,
                        "Digest validation failed!");
                // Detailed version:
                solr.addField(SolrFields.PARSE_ERROR,
                        "Digest validation failed: header value is "
                                + hashIS.getHashStream().getHeaderHash()
                                + " but calculated "
                                + hashIS.getHashStream().getHash());
            }

            final InputStream tikaInput = hashIS.getInputStream();
            // TODO: Consider adding support for GZip / Brotli compression at this point
            final String hash = hashIS.getHashStream().getHash();
            Instrument.timeRel("WARCIndexer.extract#total",
                               "WARCIndexer.extract#hashstreamwrap", hashStreamStart);
            // Set these last:
            solr.setField( SolrFields.HASH, hash );

            applyAnnotations(solr, saneURI);

            // -----------------------------------------------------
            // WARC revisit record handling:
            // -----------------------------------------------------

            // If this is a revisit record, we should just return an update to the crawl_dates (when using hashUrlId)
            if (WARCConstants.WARCRecordType.revisit.name().equalsIgnoreCase((String) header.getHeaderValue(HEADER_KEY_TYPE))) {
                solr.removeField(SolrFields.CONTENT_LENGTH); //It is 0 and would mess with statistics                                                                                
                //Copy content_type_served to content_type (no tika/droid for revisits)
                solr.addField(SolrFields.SOLR_CONTENT_TYPE, (String) solr.getFieldValue(SolrFields.CONTENT_TYPE_SERVED));
                return solr;
            }

            // -----------------------------------------------------
            // Payload duplication has been checked, ready to parse:
            // -----------------------------------------------------

            final long analyzeStart = System.nanoTime();

            // Mark the start of the payload, with a readLimit corresponding to
            // the payload size:
            tikaInput.mark( ( int ) content_length );
            
            // Pass on to other extractors as required, resetting the stream before each:
            this.wpa.analyse(archiveName, header, httpHeader, tikaInput, solr, content_length);
            Instrument.timeRel("WARCIndexer.extract#total", "WARCIndexer.extract#analyzetikainput", analyzeStart);

            // Clear up the caching of the payload:
            hashIS.cleanup();

            // -----------------------------------------------------
            // Payload analysis complete, now performing text analysis:
            // -----------------------------------------------------

            processText(solr, isTextIncluded);
        }
        Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#solrdocCreation",
                     "WARCIndexer.extract#total", start);
        String servedType = "" + solr.getField(SolrFields.CONTENT_TYPE_SERVED);
        Instrument.timeRel("WARCIndexer#content_types",
                     "WARCIndexer#" + (servedType.contains(";") ? servedType.split(";")[0] : servedType),start);
        Instrument.timeRel("WARCIndexer#content_types", start);
        return solr;
    }

    private void processText(SolrRecord solr, boolean isTextIncluded) {
        this.txa.analyse(solr);

        // Remove the Text Field if required
        if( !isTextIncluded ) {
            solr.removeField(SolrFields.SOLR_EXTRACTED_TEXT );
        } else {
            // Otherwise, decide whether to store or both store and index
            // the text:
            if (!storeText) {
                // Copy the text into the indexed (but not stored) field:
                solr.setField(SolrFields.SOLR_EXTRACTED_TEXT_NOT_STORED,
                        (String) solr.getField(
                                SolrFields.SOLR_EXTRACTED_TEXT)
                                .getFirstValue());
                // Take the text out of the original (stored) field.
                solr.removeField(SolrFields.SOLR_EXTRACTED_TEXT);
            }
        }
    }

    private void applyAnnotations(SolrRecord solr, URI saneURI) throws URIException {
        // -----------------------------------------------------
        // Apply any annotations:
        // -----------------------------------------------------
        if (ant != null) {
            try {
                ant.applyAnnotations(saneURI, solr.getSolrDocument());
            } catch (URISyntaxException e) {
                e.printStackTrace();
                log.error("Failed to annotate " + saneURI + " : " + e);
            }
        }
    }

    /**
     * Updates the provided Solr document based on envelope (ARC or WARC) headers.
     */
    private void processEnvelopeHeader(SolrRecord solr, ArchiveRecordHeader header, String targetUrl)
            throws UnsupportedEncodingException {
        // Basic metadata:
        solr.setField(SolrFields.SOURCE_FILE_OFFSET, "" + header.getOffset());
        final String filePath = header.getReaderIdentifier();//Full path of file

        //Will convert windows path to linux path. Linux paths will not be modified.
        final String linuxFilePath = FilenameUtils.separatorsToUnix(filePath);
        solr.setField(SolrFields.SOURCE_FILE_PATH, linuxFilePath);

        byte[] url_md5digest = md5
                .digest(Normalisation.sanitiseWARCHeaderValue(header.getUrl()).getBytes("UTF-8"));
        // String url_base64 =
        // Base64.encodeBase64String(fullUrl.getBytes("UTF-8"));
        final String url_md5hex = Base64.encodeBase64String(url_md5digest);
        solr.setField(SolrFields.SOLR_URL, Normalisation.sanitiseWARCHeaderValue(header.getUrl()));
        if (addNormalisedURL) {
            solr.setField( SolrFields.SOLR_URL_NORMALISED, Normalisation.canonicaliseURL(targetUrl) );
        }

        // Get the length, but beware, this value also includes the HTTP headers (i.e. it is the payload_length):
        //long content_length = header.getLength();

        // Also pull out the file extension, if any:
        final String resourceName = parseResourceName(targetUrl);
        solr.addField(SolrFields.RESOURCE_NAME, resourceName);
        solr.addField(SolrFields.CONTENT_TYPE_EXT,
                parseExtension(resourceName));


        // Prepare crawl date information:
        final String waybackDate = (header.getDate().replaceAll("[^0-9]", ""));
        final Date crawlDate = getWaybackDate(waybackDate);

        // Use an ID that ensures every URL+timestamp gets a separate
        // record:
        final String id = waybackDate + "/" + url_md5hex;
        solr.setField( SolrFields.ID, id );

        // Store the dates:
        solr.setField(SolrFields.CRAWL_DATE, formatter.format(crawlDate));
        solr.setField(SolrFields.CRAWL_YEAR, getYearFromDate(crawlDate));

        // Use the current value as the waybackDate:
        solr.setField(SolrFields.WAYBACK_DATE, WAYBACK_DATETIME.format(crawlDate.toInstant()));
    }

    /**
     * Assigns user-specified fields (institution, collection, collection ID) to the provided Solr document.
     */
    private void setUserFields(SolrRecord solr, String archiveName) {
        solr.setField(SolrFields.SOURCE_FILE, archiveName);
        // -----------------------------------------------------
        // Add user supplied Archive-It Solr fields and values:
        // -----------------------------------------------------
        solr.setField(SolrFields.INSTITUTION, WARCIndexerCommand.institution );
        solr.setField( SolrFields.COLLECTION, WARCIndexerCommand.collection );
        solr.setField( SolrFields.COLLECTION_ID, WARCIndexerCommand.collection_id );
    }

    /**
     * Adds ARC-specific headers to the provided Solr document.
     */
    private void processARCHeader(SolrRecord solr) {
        // else we're processing ARCs so nothing to filter and no
        // revisits
        solr.setField(SolrFields.SOLR_RECORD_TYPE, "arc");
    }

    /**
     * Adds WARC-specific headers to the provided Solr document.
     */
    private void processWARCHeader(ArchiveRecordHeader header, SolrRecord solr) {
        // Store WARC record type:
        solr.setField(SolrFields.SOLR_RECORD_TYPE, (String) header.getHeaderValue(HEADER_KEY_TYPE));

        //Store WARC-Record-ID
        solr.setField(SolrFields.WARC_KEY_ID, (String) header.getHeaderValue(HEADER_KEY_ID));
        solr.setField(SolrFields.WARC_IP, (String) header.getHeaderValue(HEADER_KEY_IP));
    }

    /**
     * Perform URL parsing and manipulation.
     */
    protected URI parseURL(SolrRecord solr, String fullUrl) throws URIException {
        UsableURI url = UsableURIFactory.getInstance(fullUrl);

        solr.setField(SolrFields.SOLR_URL_PATH, url.getPath());

        // Spot 'slash pages':
        if (url.getPath().equals("/") || url.getPath().equals("")
                || url.getPath().matches("/index\\.[a-z]+$")) {
            solr.setField(SolrFields.SOLR_URL_TYPE,
                    SolrFields.SOLR_URL_TYPE_SLASHPAGE);
            // Spot 'robots.txt':
        } else if (url.getPath().equalsIgnoreCase("/robots.txt")) {
            solr.setField(SolrFields.SOLR_URL_TYPE,
                    SolrFields.SOLR_URL_TYPE_ROBOTS_TXT);
        } else {
            solr.setField(SolrFields.SOLR_URL_TYPE,
                    SolrFields.SOLR_URL_TYPE_NORMAL);
        }
        // Record the host (an canonicalised), the domain
        // and the public suffix:
        String host = url.getHost();
        if (CANONICALISE_HOST)
            host = Normalisation.canonicaliseHost(host);
        solr.setField(SolrFields.SOLR_HOST, host);

        // Add the SURT host
        solr.removeField(SolrFields.SOLR_HOST_SURT);
        ImmutableList<String> levels = LinkExtractor.allLevels(host);
        if (levels != null) {
            for (String level : levels) {
                solr.addField(SolrFields.SOLR_HOST_SURT, SURT.toSURT(level));
            }
        }

        final String domain = LinkExtractor.extractPrivateSuffixFromHost(host);
        solr.setField(SolrFields.DOMAIN, domain);
        solr.setField(SolrFields.PUBLIC_SUFFIX,
                LinkExtractor.extractPublicSuffixFromHost(host));

        // Force correct escaping:
        org.apache.commons.httpclient.URI tempUri = new org.apache.commons.httpclient
                .URI(url.getEscapedURI(),
                false);

        return URI.create(tempUri.getEscapedURI());

    }

    private synchronized String getYearFromDate(Date date) {
        calendar.setTime(date);
        return Integer.toString(calendar.get(Calendar.YEAR));
    }

    private final Calendar calendar = Calendar
            .getInstance(TimeZone.getTimeZone("UTC"));

    /* ----------------------------------- */

    /**
     * If HTTP headers are present in the WARC record, they are extracted and passed on to ${@link #processHTTPHeaders}.
     * @return {@link HTTPHeader} containing extracted HTTP status and headers for the record.
     */
    private HTTPHeader processWARCHTTPHeaders(
            ArchiveRecord record, ArchiveRecordHeader warcHeader, String targetUrl, SolrRecord solr)
            throws IOException {
        String statusCode = null;
        // There are not always headers! The code should check first.
        String statusLine = HttpParser.readLine(record, "UTF-8");
        HTTPHeader httpHeaders = new HTTPHeader();

        if (statusLine != null && statusLine.startsWith("HTTP")) {
            String[] firstLine = statusLine.split(" ");
            if (firstLine.length > 1) {
                statusCode = firstLine[1].trim();
                httpHeaders.setHttpStatus(statusCode);
                try {
                    Header[] headers = HttpParser.parseHeaders(record, "UTF-8");
                    httpHeaders.addAll(headers);
                    this.processHTTPHeaders(solr, httpHeaders, targetUrl);
                } catch (ProtocolException p) {
                    log.error(
                            "ProtocolException [" + statusCode + "]: "
                                    + warcHeader.getHeaderValue(WARCConstants.HEADER_KEY_FILENAME)
                                    + "@"
                                    + warcHeader.getHeaderValue(WARCConstants.ABSOLUTE_OFFSET_KEY),
                            p);
                }
            } else {
                log.warn("Could not parse status line: " + statusLine);
            }
        } else {
            log.warn("Invalid status line: "
                    + warcHeader.getHeaderValue(WARCConstants.HEADER_KEY_FILENAME)
                    + "@"
                    + warcHeader.getHeaderValue(WARCConstants.ABSOLUTE_OFFSET_KEY));
        }
        // No need for this, as the headers have already been read from the
        // InputStream (above):
        // WARCRecordUtils.getPayload(record); ]
        return httpHeaders;
    }

    /**
     * Adds selected HTTP headers to the Solr document.
     */
    private void processHTTPHeaders(SolrRecord solr, HTTPHeader httpHeaders , String targetUrl) {
        try {
            // This is a simple test that the status code setting worked:
            int statusCodeInt = Integer.parseInt( httpHeaders.getHttpStatus() );
            if( statusCodeInt < 0 || statusCodeInt > 1000 )
                throw new Exception( "Status code out of range: " + statusCodeInt );
            // Get the other headers:
            
            for( Header h : httpHeaders ) {
              // Get the type from the server
                if (h.getName().equalsIgnoreCase(HttpHeaders.CONTENT_TYPE)
                        && solr.getField(SolrFields.CONTENT_TYPE_SERVED) == null) {
                    String servedType = h.getValue();
                    if (servedType.length() > 200)
                        servedType = servedType.substring(0, 200);
                    solr.addField(SolrFields.CONTENT_TYPE_SERVED, servedType);
                }
                // Also, grab the X-Powered-By or Server headers if present:
                if (h.getName().equalsIgnoreCase("X-Powered-By"))
                    solr.addField( SolrFields.SERVER, h.getValue() );
                if (h.getName().equalsIgnoreCase(HttpHeaders.SERVER))
                    solr.addField( SolrFields.SERVER, h.getValue() );
                if (h.getName().equalsIgnoreCase(HttpHeaders.LOCATION)){
                    String location = h.getValue(); //This can be relative and must be resolved full                  
                       solr.setField(SolrFields.REDIRECT_TO_NORM,  Normalisation.resolveRelative(targetUrl, location));
                }
                                               
            }
        } catch( NumberFormatException e ) {
            log.error( "Exception when parsing status code: " + httpHeaders.getHttpStatus() + ": " + e );
            solr.addParseException("when parsing statusCode", e);
        } catch( Exception e ) {
            log.error( "Exception when parsing headers: " + e );
            solr.addParseException("when parsing headers", e);
        }
    }

    protected static String parseResourceName(String fullUrl) {
        if( fullUrl.lastIndexOf( "/" ) != -1 ) {
            String path = fullUrl.substring(fullUrl.lastIndexOf("/") + 1);
            if( path.indexOf( "?" ) != -1 ) {
                path = path.substring( 0, path.indexOf( "?" ) );
            }
            if( path.indexOf( "&" ) != -1 ) {
                path = path.substring( 0, path.indexOf( "&" ) );
            }
            return path;
        }
        return null;
    }

    protected static String parseExtension(String path) {
        if (path != null && path.indexOf(".") != -1) {
            String ext = path.substring(path.lastIndexOf("."));
            ext = ext.toLowerCase();
            // Avoid odd/malformed extensions:
            // if( ext.contains("%") )
            // ext = ext.substring(0, path.indexOf("%"));
            ext = ext.replaceAll("[^0-9a-z]", "");
            return ext;
        }
        return null;
    }

    /**
     * Timestamp parsing, for the Crawl Date.
     */

    public static SimpleDateFormat formatter = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss'Z'");
    static {
        formatter.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
    }

    /**
     * Returns a Java Date object representing the crawled date.
     * 
     * @param timestamp
     * @return
     */
    public static Date getWaybackDate( String timestamp ) {
        Date date = new Date();
        try {
            if( timestamp.length() == 12 ) {
                date = ArchiveUtils.parse12DigitDate( timestamp );
            } else if( timestamp.length() == 14 ) {
                date = ArchiveUtils.parse14DigitDate( timestamp );
            } else if( timestamp.length() == 16 ) {
                date = ArchiveUtils.parse17DigitDate( timestamp + "0" );
            } else if( timestamp.length() >= 17 ) {
                date = ArchiveUtils.parse17DigitDate( timestamp.substring( 0, 17 ) );
            }
        } catch( ParseException p ) {
            p.printStackTrace();
        }
        return date;
    }

    /**
     * Returns a formatted String representing the crawled date.
     * 
     * @param waybackDate
     * @return
     */
    protected static String parseCrawlDate( String waybackDate ) {
        DateTimeFormatter iso_df = ISODateTimeFormat.dateTimeNoMillis()
                .withZone(DateTimeZone.UTC);
        return iso_df.print(new org.joda.time.DateTime(
                getWaybackDate(waybackDate)));
    }

    /**
     * 
     * @param timestamp
     * @return
     */
    public static String extractYear( String timestamp ) {
        // Default to 'unknown':
        String waybackYear = "unknown";
        String waybackDate = timestamp.replaceAll( "[^0-9]", "" );
        if( waybackDate != null )
            waybackYear = waybackDate.substring( 0, 4 );
        // Reject bad values by resetting to 'unknown':
        if( "0000".equals( waybackYear ) )
            waybackYear = "unknown";
        // Return
        return waybackYear;
    }

    private boolean checkUrl( String url ) {
        for( String exclude : url_excludes ) {
            if (!"".equalsIgnoreCase(exclude)
                    && url.matches(".*" + exclude + ".*")) {
                return false;
            }
        }
        return true;
    }

    private boolean checkProtocol( String url ) {
        for( String include : protocol_includes ) {
            if ("".equalsIgnoreCase(include) || url.startsWith(include)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkResponseCode( String statusCode ) {
        if( statusCode == null )
            return false;
        // Check for match:
        for( String include : response_includes ) {
            if ("".equalsIgnoreCase(include) || statusCode.startsWith(include)) {
                return true;
            }
        }
        // Exclude
        return false;
    }

    private boolean checkRecordType( String type ) {
        if (record_type_includes.contains(type)) {
                return true;
        }
        log.debug("Skipping record of type " + type);
        return false;
    }

    private boolean checkExclusionFilter( String uri ) {
        // Default to no exclusions:
        if( smef == null )
            return true;
        // Otherwise:
        ExclusionFilter ef = smef.get();
        CaptureSearchResult r = new CaptureSearchResult();
        // r.setOriginalUrl(uri);
        r.setUrlKey( uri );
        try {
            if( ef.filterObject( r ) == ExclusionFilter.FILTER_INCLUDE ) {
                return true;
            }
        } catch( Exception e ) {
            log.error( "Exclusion filtering failed with exception: " + e );
            e.printStackTrace();
        }
        log.debug( "EXCLUDING this URL due to filter: " + uri );
        // Exclude:
        return false;
    }

}
