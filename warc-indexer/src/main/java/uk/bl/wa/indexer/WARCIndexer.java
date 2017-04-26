package uk.bl.wa.indexer;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.ProtocolException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.tika.mime.MediaType;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.util.ArchiveUtils;
import org.archive.util.SurtPrefixSet;
import org.archive.wayback.accesscontrol.staticmap.StaticMapExclusionFilterFactory;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.filters.ExclusionFilter;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import uk.bl.wa.analyser.payload.WARCPayloadAnalysers;
import uk.bl.wa.analyser.text.TextAnalysers;
import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.annotation.Annotator;
import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.util.HashedCachedInputStream;
import uk.bl.wa.util.Instrument;

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
	private static Log log = LogFactory.getLog( WARCIndexer.class );

	private List<String> url_excludes;
	private List<String> protocol_includes;
	private List<String> response_includes;
	private List<String> record_type_includes;

	private MessageDigest md5 = null;
	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();

	/** */
	private boolean extractText;
	private boolean storeText;
	private boolean hashUrlId;

	/** Wayback-style URI filtering: */
	private StaticMapExclusionFilterFactory smef = null;

	/** Hook to the solr server: */
	private boolean checkSolrForDuplicates = false;
	private SolrWebServer solrServer = null;
	
	/** Payload Analysers */
	private long inMemoryThreshold;
	private long onDiskThreshold;
	private WARCPayloadAnalysers wpa;
	
	/** Text Analysers */
	private TextAnalysers txa;

	/** Annotations */
	private Annotator ant = null;

    // Paired with HtmlFeatureParsers links-extractor
    private final boolean addNormalisedURL;
    private final AggressiveUrlCanonicalizer urlNormaliser = new AggressiveUrlCanonicalizer();

	// Also canonicalise the HOST field (e.g. drop "www.")
	public static final boolean CANONICALISE_HOST = true;

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
			props.load(getClass().getResourceAsStream("/log4j-override.properties"));
			PropertyConfigurator.configure(props);
		} catch (IOException e1) {
			log.error("Failed to load log4j config from properties file.");
		}

		// Optional configurations:
		this.extractText = conf.getBoolean( "warc.index.extract.content.text" );
		log.info("Extract text = " + extractText);
		this.storeText = conf
				.getBoolean("warc.index.extract.content.text_stored");
		log.info("Store text = " + storeText);
		this.hashUrlId = conf.getBoolean( "warc.solr.use_hash_url_id" );
		log.info("hashUrlId = " + hashUrlId);
        addNormalisedURL = conf.hasPath(HtmlFeatureParser.CONF_LINKS_NORMALISE) ?
                conf.getBoolean(HtmlFeatureParser.CONF_LINKS_NORMALISE) :
                HtmlFeatureParser.DEFAULT_LINKS_NORMALISE;
		this.checkSolrForDuplicates = conf.getBoolean("warc.solr.check_solr_for_duplicates");
		if( this.hashUrlId == false && this.checkSolrForDuplicates == true ) {
			log.warn("Checking Solr for duplicates may not work as expected when using the timestamp+md5(URL) key.");
			log.warn("You need to use the payload-hash+md5(URL) key option to resolve revisit records.");
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
		log.info("Hashing & Caching thresholds are: < "+this.inMemoryThreshold+" in memory, < "+this.onDiskThreshold+" on disk.");
		
		// Set up analysers
		log.info("Setting up analysers...");
		this.wpa = new WARCPayloadAnalysers(conf);
		this.txa = new TextAnalysers(conf);
		
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
		ArchiveRecordHeader header = record.getHeader();
		SolrRecord solr = new SolrRecord(archiveName, header);
		
		if( !header.getHeaderFields().isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) ) {
				log.debug("Looking at "
						+ header.getHeaderValue(HEADER_KEY_TYPE));

				if( !checkRecordType( ( String ) header.getHeaderValue( HEADER_KEY_TYPE ) ) ) {
					return null;
				}
				// Store WARC record type:
				solr.setField(SolrFields.SOLR_RECORD_TYPE,
						(String) header.getHeaderValue(HEADER_KEY_TYPE));
			} else {
				// else we're processing ARCs so nothing to filter and no
				// revisits
				solr.setField(SolrFields.SOLR_RECORD_TYPE, "arc");
			}

			if( header.getUrl() == null )
				return null;
			String fullUrl = header.getUrl();
			log.debug("Current heap usage: "
					+ FileUtils.byteCountToDisplaySize(Runtime.getRuntime()
							.totalMemory()));
			log.debug("Processing " + fullUrl + " from " + archiveName);

			// Check the filters:
			if( this.checkProtocol( fullUrl ) == false )
				return null;
			if( this.checkUrl( fullUrl ) == false )
				return null;
			if( this.checkExclusionFilter( fullUrl ) == false )
				return null;

			// --- Basic headers ---

			// Basic metadata:
			solr.setField(SolrFields.SOURCE_FILE, archiveName);
			solr.setField(SolrFields.SOURCE_FILE_OFFSET,
					"" + header.getOffset());
			byte[] url_md5digest = md5.digest(fullUrl.getBytes("UTF-8"));
			// String url_base64 =
			// Base64.encodeBase64String(fullUrl.getBytes("UTF-8"));
			String url_md5hex = Base64.encodeBase64String(url_md5digest);
			solr.setField( SolrFields.SOLR_URL, fullUrl );
            if (addNormalisedURL) {
                solr.setField( SolrFields.SOLR_URL_NORMALISED, urlNormaliser.canonicalize(fullUrl) );
            }
			// Get the length, but beware, this value also includes the HTTP headers (i.e. it is the payload_length):
			long content_length = header.getLength();

			// Also pull out the file extension, if any:
			String resourceName = parseResourceName(fullUrl);
			solr.addField(SolrFields.RESOURCE_NAME, resourceName);
			solr.addField(SolrFields.CONTENT_TYPE_EXT,
					parseExtension(resourceName));
			// Strip down very long URLs to avoid "org.apache.commons.httpclient.URIException: Created (escaped) uuri > 2083"
			// Trac #2271: replace string-splitting with URI-based methods.
			URL url = null;
			if( fullUrl.length() > 2000 )
				fullUrl = fullUrl.substring( 0, 2000 );
			try {
				url = new URL(fullUrl);
			} catch (MalformedURLException e) {
				// Some URIs causing problem, so try the canonicalizer; in which
				// case try with the full URL.
				log.error(e.getMessage());
				try {
					url = new URL("http://" + canon.urlStringToKey(fullUrl));
				} catch (Exception e2) {
					// If this fails, abandon all hope.
					log.error(e2.getMessage());
					return null;
				}
			}
			// Spot 'slash pages':
			if (url.getPath().equals("/") || url.getPath().equals("")
					|| url.getPath().matches("/index\\.[a-z]+$")) {
				solr.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_SLASHPAGE );
			// Spot 'robots.txt':
			} else if (url.getPath().equalsIgnoreCase("/robots.txt")) {
				solr.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_ROBOTS_TXT );
			} else {
				solr.setField(SolrFields.SOLR_URL_TYPE,
						SolrFields.SOLR_URL_TYPE_NORMAL);
			}
			// Record the host (an canonicalised), the domain
			// and the public suffix:
			String host = url.getHost();
			if (CANONICALISE_HOST)
				host = canon.urlStringToKey(host).replace("/", "");
			solr.setField( SolrFields.SOLR_HOST, host );
			solr.setField( SolrFields.DOMAIN, LinkExtractor.extractPrivateSuffixFromHost( host ) );
			solr.setField( SolrFields.PUBLIC_SUFFIX, LinkExtractor.extractPublicSuffixFromHost( host ) );

            Instrument.timeRel("WARCIndexer.extract#total",
                               "WARCIndexer.extract#archeaders", start);

			InputStream tikainput = null;

			// Only parse HTTP headers for HTTP URIs
			if( fullUrl.startsWith( "http" ) ) {
				// Parse HTTP headers:
				String statusCode = null;
				if( record instanceof WARCRecord ) {
					// There are not always headers! The code should check first.
					String statusLine = HttpParser.readLine( record, "UTF-8" );
					if( statusLine != null && statusLine.startsWith( "HTTP" ) ) {
						String firstLine[] = statusLine.split( " " );
						if( firstLine.length > 1 ) {
							statusCode = firstLine[ 1 ].trim();
							try {
								this.processHeaders( solr, statusCode, HttpParser.parseHeaders( record, "UTF-8" ) );
							} catch( ProtocolException p ) {
								log.error( "ProtocolException [" + statusCode + "]: " + header.getHeaderValue( WARCConstants.HEADER_KEY_FILENAME ) + "@" + header.getHeaderValue( WARCConstants.ABSOLUTE_OFFSET_KEY ), p );
							}
						} else {
							log.warn( "Could not parse status line: " + statusLine );
						}
					} else {
						log.warn( "Invalid status line: " + header.getHeaderValue( WARCConstants.HEADER_KEY_FILENAME ) + "@" + header.getHeaderValue( WARCConstants.ABSOLUTE_OFFSET_KEY ) );
					}
					// No need for this, as the headers have already been read from the InputStream (above):
					// WARCRecordUtils.getPayload(record);
					tikainput = record;
				} else if( record instanceof ARCRecord ) {
					ARCRecord arcr = ( ARCRecord ) record;
					statusCode = "" + arcr.getStatusCode();
					this.processHeaders( solr, statusCode, arcr.getHttpHeaders() );
					arcr.skipHttpHeader();
					tikainput = arcr;
				} else {
					log.error( "FAIL! Unsupported archive record type." );
					return solr;
				}

				// Skip recording non-content URLs (i.e. 2xx responses only please):
				if( this.checkResponseCode( statusCode ) == false ) {
					log.debug( "Skipping this record based on status code " + statusCode + ": " + header.getUrl() );
					return null;
				}
			}
			
			// Update the content_length based on what's available:
			content_length = tikainput.available();
			// Record the length:
			solr.setField(SolrFields.CONTENT_LENGTH, ""+content_length);
			
			// -----------------------------------------------------
			// Headers have been processed, payload ready to cache:
			// -----------------------------------------------------
			
			// Create an appropriately cached version of the payload, to allow analysis.
            final long hashStreamStart = System.nanoTime();
			HashedCachedInputStream hcis = new HashedCachedInputStream(header, tikainput, content_length );
			tikainput = hcis.getInputStream();
			String hash = hcis.getHash();
            Instrument.timeRel("WARCIndexer.extract#total",
                               "WARCIndexer.extract#hashstreamwrap", hashStreamStart);

			// Prepare crawl date information:
			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );
			Date crawlDate =  getWaybackDate( waybackDate );
			String crawlDateString = parseCrawlDate(waybackDate);
			
			// Optionally use a hash-based ID to store only one version of a URL:
			String id = null;
			if( hashUrlId ) {
				id = hash + "/" + url_md5hex;
			} else {
				id = url_md5hex + "/" + waybackDate;
			}
			// Set these last:
			solr.setField( SolrFields.ID, id );
			solr.setField( SolrFields.HASH, hash );

			// -----------------------------------------------------
			// Payload has been cached, ready to check crawl dates:
			// -----------------------------------------------------
			
			HashSet<Date> currentCrawlDates = new HashSet<Date>();
			// If we are collapsing records based on hash:
			if (hashUrlId) {
				// Query for currently known crawl dates:
				if (this.checkSolrForDuplicates && solrServer != null) {
					SolrQuery q = new SolrQuery("id:\"" + id + "\"");
					q.addField(SolrFields.CRAWL_DATES);
					try {
						QueryResponse results = solrServer.query(q);
						if (results.getResults().size() > 0) {
							SolrDocument fr = results.getResults().get(0);
							if (fr.containsKey(SolrFields.CRAWL_DATES)) {
								for (Object cds : fr
										.getFieldValues(SolrFields.CRAWL_DATES)) {
									currentCrawlDates.add((Date) cds);
								}
							}
						} else {
							log.debug("No matching entries found.");
						}
					} catch (SolrServerException e) {
						e.printStackTrace();
						// FIXME retry?
					}
				}

				// Is the current date unknown? (inc. no-solr-check case):
				if (!currentCrawlDates.contains(crawlDate)) {
					// Dates to be merged under the CRAWL_DATES field:
					solr.mergeField(SolrFields.CRAWL_DATES, crawlDateString);
					solr.mergeField(SolrFields.CRAWL_YEARS,
							extractYear(header.getDate()));
				} else {
					// Otherwise, ensure the all the known dates (i.e. including
					// this one) are copied over:
					for (Date ccd : currentCrawlDates) {
						solr.addField(SolrFields.CRAWL_DATES,
								formatter.format(ccd));
						solr.addField(SolrFields.CRAWL_YEARS,
								getYearFromDate(ccd));
					}
					// TODO This could optionally skip re-submission instead?
				}
			}
			
			// Sort the dates and find the earliest:
			List<Date> dateList = new ArrayList<Date>(currentCrawlDates);
			dateList.add(crawlDate);
			Collections.sort(dateList);
			Date firstDate = dateList.get(0);
			solr.setField(SolrFields.CRAWL_DATE,
					formatter.format(firstDate));
			solr.setField( SolrFields.CRAWL_YEAR, getYearFromDate(firstDate) );
			
			// Use the current value as the waybackDate:
			solr.setField( SolrFields.WAYBACK_DATE, waybackDate );
			
			// If this is a revisit record, we should just return an update to the crawl_dates:
			if (WARCConstants.WARCRecordType.revisit.name().equalsIgnoreCase(
					(String) header.getHeaderValue(HEADER_KEY_TYPE))) {
				if( currentCrawlDates.contains(crawlDate) ) {
					return null;
				}
				SolrRecord revisited = new SolrRecord();
				revisited.setField( SolrFields.ID, id );
				// Store crawl-date appropriately depending on whether records
				// are collapsing on (hash+url) or not:
				if (hashUrlId) {
					revisited.mergeField(SolrFields.CRAWL_DATES,
							crawlDateString);
					revisited.mergeField(SolrFields.CRAWL_YEARS,
							extractYear(header.getDate()));
				} else {
					revisited.setField(SolrFields.CRAWL_DATE, crawlDateString);
					revisited.setField(SolrFields.CRAWL_YEAR,
						extractYear(header.getDate()));
				}
				revisited.setField(SolrFields.SOLR_URL, fullUrl);
				revisited.setField(SolrFields.WAYBACK_DATE, waybackDate);
				String payloadDigest = (String) header
						.getHeaderValue(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST);
				revisited.setField(SolrFields.HASH, payloadDigest);
				revisited.setField(SolrFields.SOLR_RECORD_TYPE,
						(String) header.getHeaderValue(HEADER_KEY_TYPE));
				return revisited;
			}
			
			// -----------------------------------------------------
			// Apply any annotations:
			// -----------------------------------------------------
			if (ant != null) {
				try {
					ant.applyAnnotations(url.toURI(), solr.getSolrDocument());
				} catch (URISyntaxException e) {
					e.printStackTrace();
					log.error("Failed to annotate " + url + " : " + e);
				}
			}

			// -----------------------------------------------------
			// Payload duplication has been checked, ready to parse:
			// -----------------------------------------------------

            final long analyzeStart = System.nanoTime();
			// Mark the start of the payload.
			tikainput.mark( ( int ) content_length );
			
			// Pass on to other extractors as required, resetting the stream before each:
			this.wpa.analyse(header, tikainput, solr);
            Instrument.timeRel("WARCIndexer.extract#total", "WARCIndexer.extract#analyzetikainput", analyzeStart);


			// Clear up the caching of the payload:
			hcis.cleanup();

			// Derive normalised/simplified content type:
			processContentType(solr, header, content_length);

			// -----------------------------------------------------
			// Payload analysis complete, now performing text analysis:
			// -----------------------------------------------------
			
			this.txa.analyse(solr);

			// Remove the Text Field if required
			if( !isTextIncluded ) {
				solr.removeField( SolrFields.SOLR_EXTRACTED_TEXT );

			} else {
				// Otherwise, decide whether to store or both store and index
				// the text:
				if (storeText == false) {
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
        Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#solrdocCreation",
                           "WARCIndexer.extract#total", start);
        return solr;
	}

	/**
	 * @param firstDate
	 * @return
	 */
	private synchronized String getYearFromDate(Date date) {
		calendar.setTime(date);
		return Integer.toString(calendar.get(Calendar.YEAR));
	}
    private final Calendar calendar = Calendar.getInstance();

	/* ----------------------------------- */

	private void processHeaders( SolrRecord solr, String statusCode, Header[] httpHeaders ) {
		try {
			// This is a simple test that the status code setting worked:
			int statusCodeInt = Integer.parseInt( statusCode );
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
			}
		} catch( NumberFormatException e ) {
			log.error( "Exception when parsing status code: " + statusCode + ": " + e );
			solr.addParseException("when parsing statusCode", e);
		} catch( Exception e ) {
			log.error( "Exception when parsing headers: " + e );
			solr.addParseException("when parsing headers", e);
		}
	}


	/**
	 * 
	 * @param fullUrl
	 * @return
	 */
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

	/**
	 * 
	 * @param fullUrl
	 * @return
	 */
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

	/**
	 * 
	 * @param solr
	 * @param header
	 * @param content_length
	 */
	private void processContentType(SolrRecord solr,
			ArchiveRecordHeader header, long content_length) {
		// Get the current content-type:
		String contentType = ( String ) solr.getFieldValue( SolrFields.SOLR_CONTENT_TYPE );

		// Store the raw content type from Tika:
		solr.setField( SolrFields.CONTENT_TYPE_TIKA, contentType );

		// Also get the other content types:
		MediaType mt_tika = MediaType.parse( contentType );
		if( solr.getField( SolrFields.CONTENT_TYPE_DROID ) != null ) {
			MediaType mt_droid = MediaType.parse( ( String ) solr.getField( SolrFields.CONTENT_TYPE_DROID ).getFirstValue() );
			if( mt_tika == null || mt_tika.equals( MediaType.OCTET_STREAM ) ) {
				contentType = mt_droid.toString();
			} else if( mt_droid.getBaseType().equals( mt_tika.getBaseType() ) && mt_droid.getParameters().get( "version" ) != null ) {
				// Union of results:
				mt_tika = new MediaType( mt_tika, mt_droid.getParameters() );
				contentType = mt_tika.toString();
			}
			if( mt_droid.getParameters().get( "version" ) != null ) {
				solr.addField( SolrFields.CONTENT_VERSION, mt_droid.getParameters().get( "version" ) );
			}
		}

		// Allow header MIME
		if( contentType != null && contentType.isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( "WARC-Identified-Payload-Type" ) ) {
				contentType = ( ( String ) header.getHeaderFields().get( "WARC-Identified-Payload-Type" ) );
			} else {
				contentType = header.getMimetype();
			}
		}

		// Determine content type:
		if( contentType != null )
			solr.setField( SolrFields.FULL_CONTENT_TYPE, contentType );
		
		// If zero-length, then change to application/x-empty for the 'content_type' field.
		if (content_length == 0)
			contentType = "application/x-empty";

		// Content-Type can still be null
		if( contentType != null ) {
			// Strip parameters out of main type field:
			solr.setField( SolrFields.SOLR_CONTENT_TYPE, contentType.replaceAll( ";.*$", "" ) );

			// Also add a more general, simplified type, as appropriate:
			if( contentType.matches( "^image/.*$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "image" );
			} else if( contentType.matches( "^audio/.*$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "audio" );
			} else if( contentType.matches( "^video/.*$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "video" );
			} else if( contentType.matches( "^text/htm.*$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "html" );
			} else if( contentType.matches( "^application/pdf.*$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "pdf" );
			} else if( contentType.matches( "^.*word$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "word" );
			} else if( contentType.matches( "^.*excel$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "excel" );
			} else if( contentType.matches( "^.*powerpoint$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "powerpoint" );
			} else if( contentType.matches( "^text/plain.*$" ) ) {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "text" );
			} else {
				solr.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "other" );
			}

			// Remove text from JavaScript, CSS, ...
			if( contentType.startsWith( "application/javascript" ) || contentType.startsWith( "text/javascript" ) || contentType.startsWith( "text/css" ) ) {
				solr.removeField( SolrFields.SOLR_EXTRACTED_TEXT );
			}
		}
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
