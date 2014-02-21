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

import static org.archive.io.warc.WARCConstants.HEADER_KEY_PAYLOAD_DIGEST;
import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.RESPONSE;
import static org.archive.io.warc.WARCConstants.REVISIT;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.ProtocolException;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.archive.util.ArchiveUtils;
import org.archive.util.Base32;
import org.archive.wayback.accesscontrol.staticmap.StaticMapExclusionFilterFactory;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.filters.ExclusionFilter;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

import uk.bl.wa.extract.LanguageDetector;
import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.nanite.droid.DroidDetector;
import uk.bl.wa.parsers.ApachePreflightParser;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.parsers.XMLRootNamespaceParser;
import uk.bl.wa.sentimentalj.Sentiment;
import uk.bl.wa.sentimentalj.SentimentalJ;
import uk.bl.wa.util.PostcodeGeomapper;
import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;
import uk.bl.wa.util.solr.TikaExtractor;
import uk.gov.nationalarchives.droid.command.action.CommandExecutionException;
import antlr.Parser;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import eu.scape_project.bitwiser.utils.FuzzyHash;
import eu.scape_project.bitwiser.utils.SSDeep;

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

	private static final Pattern postcodePattern = Pattern.compile( "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}" );

	private MessageDigest digest = MessageDigest.getInstance( "SHA-1" );
	private TikaExtractor tika = null;
	private DroidDetector dd = null;
	private boolean runDroid = true;
	private boolean droidUseBinarySignaturesOnly = false;
	private boolean passUriToFormatTools = false;

	private MessageDigest md5 = null;
	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();

	/** */
	private LanguageDetector ld = new LanguageDetector();
	/** */
	private HtmlFeatureParser hfp = new HtmlFeatureParser();
	private boolean extractLinkDomains = true;
	private boolean extractLinkHosts = true;
	private boolean extractLinks = false;
	private boolean extractText = true;
	private boolean extractElementsUsed = true;
	private boolean extractContentFirstBytes = true;
	private int firstBytesLength = 32;
	private long bufferSize = ( 20L * 1024L * 1024L );

	/** */
	private ApachePreflightParser app = new ApachePreflightParser();
	private boolean extractApachePreflightErrors = true;

	/** */
	private XMLRootNamespaceParser xrns = new XMLRootNamespaceParser();
	private boolean extractXMLRootNamespace = true;

	/** */
	private SentimentalJ sentij = new SentimentalJ();

	/** */
	private PostcodeGeomapper pcg = new PostcodeGeomapper();

	/** Wayback-style URI filtering: */
	StaticMapExclusionFilterFactory smef = null;

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
		// Optional configurations:
		this.extractLinks = conf.getBoolean( "warc.index.extract.linked.resources" );
		this.extractLinkHosts = conf.getBoolean( "warc.index.extract.linked.hosts" );
		this.extractLinkDomains = conf.getBoolean( "warc.index.extract.linked.domains" );
		this.extractText = conf.getBoolean( "warc.index.extract.content.text" );
		this.extractElementsUsed = conf.getBoolean( "warc.index.extract.content.elements_used" );
		this.extractApachePreflightErrors = conf.getBoolean( "warc.index.extract.content.extractApachePreflightErrors" );
		this.extractContentFirstBytes = conf.getBoolean( "warc.index.extract.content.first_bytes.enabled" );
		this.firstBytesLength = conf.getInt( "warc.index.extract.content.first_bytes.num_bytes" );
		this.runDroid = conf.getBoolean( "warc.index.id.droid.enabled" );
		this.passUriToFormatTools = conf.getBoolean( "warc.index.id.useResourceURI" );
		this.droidUseBinarySignaturesOnly = conf.getBoolean( "warc.index.id.droid.useBinarySignaturesOnly" );
		// URLs to exclude:
		this.url_excludes = conf.getStringList( "warc.index.extract.url_exclude" );
		// Protocols to include:
		this.protocol_includes = conf.getStringList( "warc.index.extract.protocol_include" );
		// Response codes to include:
		this.response_includes = conf.getStringList( "warc.index.extract.response_include" );
		this.bufferSize = conf.getLong( "warc.index.extract.buffer_size" );

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
		// Attempt to set up Droid:
		try {
			dd = new DroidDetector();
			dd.setBinarySignaturesOnly( droidUseBinarySignaturesOnly );
			dd.setMaxBytesToScan( bufferSize );
		} catch( CommandExecutionException e ) {
			e.printStackTrace();
			dd = null;
		}
		tika = new TikaExtractor( conf );
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
		ArchiveRecordHeader header = record.getHeader();
		SolrRecord solr = new SolrRecord();

		if( !header.getHeaderFields().isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) ) {
				if( !( header.getHeaderValue( HEADER_KEY_TYPE ).equals( RESPONSE ) || header.getHeaderValue( HEADER_KEY_TYPE ).equals( REVISIT ) ) ) {
					return null;
				}
			} // else we're processing ARCs

			if( header.getUrl() == null )
				return null;
			String fullUrl = header.getUrl();

			// Check the filters:
			if( this.checkProtocol( fullUrl ) == false )
				return null;
			if( this.checkUrl( fullUrl ) == false )
				return null;
			if( this.checkExclusionFilter( fullUrl ) == false )
				return null;

			// --- Basic headers ---

			// Dates
			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );
			solr.doc.setField( SolrFields.WAYBACK_DATE, waybackDate );
			solr.doc.setField( SolrFields.CRAWL_YEAR, extractYear( header.getDate() ) );
			solr.doc.setField( SolrFields.CRAWL_DATE, parseCrawlDate( waybackDate ) );

			// Basic metadata:
			byte[] md5digest = md5.digest( fullUrl.getBytes( "UTF-8" ) );
			String md5hex = new String( Base64.encodeBase64( md5digest ) );
			solr.doc.setField( SolrFields.ID, waybackDate + "/" + md5hex );
			solr.doc.setField( SolrFields.ID_LONG, Long.parseLong( waybackDate + "00" ) + ( ( md5digest[ 1 ] << 8 ) + md5digest[ 0 ] ) );
			solr.doc.setField( SolrFields.SOLR_URL, fullUrl );
			// Get the length, but beware, this value also includes the HTTP headers (i.e. it is the payload_length):
			long content_length = header.getLength();

			// Also pull out the file extension, if any:
			solr.doc.addField( SolrFields.CONTENT_TYPE_EXT, parseExtension( fullUrl ) );
			// Strip down very long URLs to avoid "org.apache.commons.httpclient.URIException: Created (escaped) uuri > 2083"
			// Trac #2271: replace string-splitting with URI-based methods.
			URL url = null;
			if( fullUrl.length() > 2000 )
				fullUrl = fullUrl.substring( 0, 2000 );
			try {
				url = new URL( "http://" + canon.urlStringToKey( fullUrl ) );
			} catch( URIException u ) {
				// Some URIs still causing problems in canonicalizer; in which case try with the full URL.
				log.error( u.getMessage() );
				try {
					url = new URL( fullUrl );
				} catch( MalformedURLException e ) {
					// If this fails, abandon all hope.
					log.error( e.getMessage() );
					return null;
				}
			} catch( MalformedURLException e ) {
				log.error( e.getMessage() );
				return null;
			}
			// Spot 'slash pages':
			if( url.getPath().equals( "/" ) || url.getPath().equals( "" ) || url.getPath().matches( "/index\\.[a-z]+$" ) )
				solr.doc.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_SLASHPAGE );
			// Spot 'robots.txt':
			if( url.getPath().equals( "/robots.txt" ) )
				solr.doc.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_ROBOTS_TXT );
			// Record the domain (strictly, the host):
			String host = url.getHost();
			solr.doc.setField( SolrFields.SOLR_HOST, host );
			solr.doc.setField( SolrFields.DOMAIN, LinkExtractor.extractPrivateSuffixFromHost( host ) );
			solr.doc.setField( SolrFields.PUBLIC_SUFFIX, LinkExtractor.extractPublicSuffixFromHost( host ) );

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
					log.info( "Skipping this record based on status code " + statusCode + ": " + header.getUrl() );
					return null;
				}
			}
			
			// Update the content_length based on what's available:
			content_length = tikainput.available();

			// -----------------------------------------------------
			// Parse payload using Tika:
			// -----------------------------------------------------

			// Mark the start of the payload, and then run Tika on it:
			tikainput = new BufferedInputStream( new BoundedInputStream( tikainput, bufferSize ), ( int ) bufferSize );
			tikainput.mark( ( int ) header.getLength() );
			if( passUriToFormatTools ) {
				solr = tika.extract( solr, tikainput, header.getUrl() );
			} else {
				solr = tika.extract( solr, tikainput, null );
			}
			
			// Record the length:
			solr.setField(SolrFields.CONTENT_LENGTH, ""+content_length);

			// TODO: ArchiveRecordHeader.getDigest() returning null for (W)ARCs.
			// TODO: At the very least we should calculate the hash on the whole InputStream.
			String hash = null;
			try {
				tikainput.reset();
				if( header.getHeaderFieldKeys().contains( HEADER_KEY_PAYLOAD_DIGEST ) ) {
					hash = ( String ) header.getHeaderValue( HEADER_KEY_PAYLOAD_DIGEST );
				} else {
					DigestInputStream dinput = new DigestInputStream( tikainput, digest );
					byte[] dummy = new byte[ 4096 ];
					while( dinput.read( dummy ) > 0 ) {
						// Do nothing
					}
					hash = "sha1:" + Base32.encode( digest.digest() );
				}
			} catch( Exception i ) {
				log.error( "Hashing: " + header.getUrl() + "@" + header.getOffset(), i );
			}
			// Set these last: ARC records must be read in full to calculate the hash.
			solr.doc.setField( SolrFields.HASH, hash );
			solr.doc.setField( SolrFields.HASH_AND_URL, hash + "_" + fullUrl );

			// Pull out the first few bytes, to hunt for new format by magic:
			try {
				tikainput.reset();
				byte[] ffb = new byte[ this.firstBytesLength ];
				int read = tikainput.read( ffb );
				if( read >= 4 ) {
					String hexBytes = Hex.encodeHexString( ffb );
					solr.addField( SolrFields.CONTENT_FFB, hexBytes.substring( 0, 2 * 4 ) );
					StringBuilder separatedHexBytes = new StringBuilder();
					for( String hexByte : Splitter.fixedLength( 2 ).split( hexBytes ) ) {
						separatedHexBytes.append( hexByte );
						separatedHexBytes.append( " " );
					}
					if( this.extractContentFirstBytes ) {
						solr.addField( SolrFields.CONTENT_FIRST_BYTES, separatedHexBytes.toString().trim() );
					}
				}
			} catch( IOException i ) {
				log.error( i + ": " + i.getMessage() + ";ffb; " + header.getUrl() + "@" + header.getOffset() );
			}

			// Also run DROID (restricted range):
			if( dd != null && runDroid == true ) {
				try {
					tikainput.reset();
					// Pass the URL in so DROID can fall back on that:
					Metadata metadata = new Metadata();
					if( passUriToFormatTools ) {
						UURI uuri = UURIFactory.getInstance( fullUrl );
						// Droid seems unhappy about spaces in filenames, so hack to avoid:
						String cleanUrl = uuri.getName().replace( " ", "+" );
						metadata.set( Metadata.RESOURCE_NAME_KEY, cleanUrl );
					}
					// Run Droid:
					MediaType mt = dd.detect( tikainput, metadata );
					solr.addField( SolrFields.CONTENT_TYPE_DROID, mt.toString() );
				} catch( Exception i ) {
					// Note that DROID complains about some URLs with an IllegalArgumentException.
					log.error( i + ": " + i.getMessage() + ";dd; " + fullUrl + " @" + header.getOffset() );
				}
			}

			// Derive normalised/simplified content type:
			processContentType( solr, header );

			// Pass on to other extractors as required, resetting the stream before each:
			// Entropy, compressibility, fussy hashes, etc.
			Metadata metadata = new Metadata();
			try {
				tikainput.reset();
				// JSoup link extractor for (x)html, deposit in 'links' field.
				String mime = ( String ) solr.doc.getField( SolrFields.SOLR_CONTENT_TYPE ).getValue();
				HashMap<String, String> hosts = new HashMap<String, String>();
				HashMap<String, String> suffixes = new HashMap<String, String>();
				HashMap<String, String> domains = new HashMap<String, String>();
				if( mime.startsWith( "text" ) ) {
					// JSoup NEEDS the URL to function:
					metadata.set( Metadata.RESOURCE_NAME_KEY, header.getUrl() );
					ParseRunner parser = new ParseRunner( hfp, tikainput, metadata, solr );
					Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
					try {
						thread.start();
						thread.join( 30000L );
						thread.interrupt();
					} catch( Exception e ) {
						log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
						solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing as HTML: " + e.getMessage() );
					}

					// Process links:
					String links_list = metadata.get( HtmlFeatureParser.LINK_LIST );
					if( links_list != null ) {
						String lhost, ldomain, lsuffix;
						for( String link : links_list.split( " " ) ) {
							lhost = LinkExtractor.extractHost( link );
							if( !lhost.equals( LinkExtractor.MALFORMED_HOST ) ) {
								hosts.put( lhost, "" );
							}
							lsuffix = LinkExtractor.extractPublicSuffix( link );
							if( lsuffix != null ) {
								suffixes.put( lsuffix, "" );
							}
							ldomain = LinkExtractor.extractPrivateSuffix( link );
							if( ldomain != null ) {
								domains.put( ldomain, "" );
							}
							// Also store actual resource-level links:
							if( this.extractLinks )
								solr.addField( SolrFields.SOLR_LINKS, link );
						}
						// Store the data from the links:
						Iterator<String> iterator = null;
						if( this.extractLinkHosts ) {
							iterator = hosts.keySet().iterator();
							while( iterator.hasNext() ) {
								solr.addField( SolrFields.SOLR_LINKS_HOSTS, iterator.next() );
							}
						}
						if( this.extractLinkDomains ) {
							iterator = domains.keySet().iterator();
							while( iterator.hasNext() ) {
								solr.addField( SolrFields.SOLR_LINKS_DOMAINS, iterator.next() );
							}
						}
						iterator = suffixes.keySet().iterator();
						while( iterator.hasNext() ) {
							solr.addField( SolrFields.SOLR_LINKS_PUBLIC_SUFFIXES, iterator.next() );
						}
					}
					// Process element usage:
					if( this.extractElementsUsed ) {
						String[] de = metadata.getValues( HtmlFeatureParser.DISTINCT_ELEMENTS );
						if( de != null ) {
							for( String e : de ) {
								solr.addField( SolrFields.ELEMENTS_USED, e );
							}
						}
					}
					for( String lurl : metadata.getValues( Metadata.LICENSE_URL ) ) {
						solr.addField( SolrFields.LICENSE_URL, lurl );
					}

				} else if( mime.startsWith( "image" ) ) {
					// TODO Extract image properties.

				} else if( mime.startsWith( "application/pdf" ) ) {
					if( extractApachePreflightErrors ) {
						metadata.set( Metadata.RESOURCE_NAME_KEY, header.getUrl() );
						ParseRunner parser = new ParseRunner( app, tikainput, metadata, solr );
						Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
						try {
							thread.start();
							thread.join( 30000L );
							thread.interrupt();
						} catch( Exception e ) {
							log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
							solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing with Apache Preflight: " + e.getMessage() );
						}

						String isValid = metadata.get( ApachePreflightParser.PDF_PREFLIGHT_VALID );
						solr.addField( "pdf_valid_pdfa_s", isValid );
						String[] errors = metadata.getValues( ApachePreflightParser.PDF_PREFLIGHT_ERRORS );
						if( errors != null ) {
							for( String error : errors ) {
								solr.addField( "pdf_pdfa_errors_ss", error );
							}
						}
					}
				} else if( mime.startsWith("application/xml") || mime.startsWith("text/xml") ) {
					// Also attempt to grab the XML Root NS:
					if( this.extractXMLRootNamespace ) {
						ParseRunner parser = new ParseRunner( xrns, tikainput, metadata,solr );
						Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
						try {
							thread.start();
							thread.join( 30000L );
							thread.interrupt();
						} catch( Exception e ) {
							log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
							solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing for XML Root Namespace: " + e.getMessage() );
						}
						solr.doc.addField( "xml_ns_root_s", metadata.get(XMLRootNamespaceParser.XML_ROOT_NS));
					}
				} else {
					log.info("No specific additional parser for: "+mime);
				}
			} catch( Exception i ) {
				log.error( i + ": " + i.getMessage() + ";x; " + header.getUrl() + "@" + header.getOffset() );
			}

			// --- The following extractors don't need to re-read the payload ---

			// Pull out the text:
			if( solr.doc.getField( SolrFields.SOLR_EXTRACTED_TEXT ) != null ) {
				String text = ( String ) solr.doc.getField( SolrFields.SOLR_EXTRACTED_TEXT ).getFirstValue();
				text = text.trim();
				if( !"".equals( text ) ) {
					/* ---------------------------------------------------------- */

					if( metadata.get( Metadata.CONTENT_LANGUAGE ) == null ) {
						String li = ld.detectLanguage( text );
						if( li != null )
							solr.addField( SolrFields.CONTENT_LANGUAGE, li );
					} else {
						solr.addField( SolrFields.CONTENT_LANGUAGE, metadata.get( Metadata.CONTENT_LANGUAGE ) );
					}

					/* ---------------------------------------------------------- */

					// Sentiment Analysis:
					int sentilen = 10000;
					if( sentilen > text.length() )
						sentilen = text.length();
					String sentitext = text.substring( 0, sentilen );
					// metadata.get(HtmlFeatureParser.FIRST_PARAGRAPH);

					Sentiment senti = sentij.analyze( sentitext );
					double sentilog = Math.signum( senti.getComparative() ) * ( Math.log( 1.0 + Math.abs( senti.getComparative() ) ) / 40.0 );
					int sentii = ( int ) ( SolrFields.SENTIMENTS.length * ( 0.5 + sentilog ) );
					if( sentii < 0 ) {
						log.debug( "Caught a sentiment rating less than zero: " + sentii + " from " + sentilog );
						sentii = 0;
					}
					if( sentii >= SolrFields.SENTIMENTS.length ) {
						log.debug( "Caught a sentiment rating too large to be in range: " + sentii + " from " + sentilog );
						sentii = SolrFields.SENTIMENTS.length - 1;
					}
					// if( sentii != 3 )
					// log.debug("Got sentiment: " + sentii+" "+sentilog+" "+ SolrFields.SENTIMENTS[sentii] );
					// Map to sentiment scale:
					solr.addField( SolrFields.SENTIMENT, SolrFields.SENTIMENTS[ sentii ] );
					solr.addField( SolrFields.SENTIMENT_SCORE, "" + senti.getComparative() );

					/* ---------------------------------------------------------- */

					// Postcode Extractor (based on text extracted by Tika)
					Matcher pcm = postcodePattern.matcher( text );
					Set<String> pcs = new HashSet<String>();
					while( pcm.find() )
						pcs.add( pcm.group() );
					for( String pc : pcs ) {
						solr.addField( SolrFields.POSTCODE, pc );
						String pcd = pc.substring( 0, pc.lastIndexOf( " " ) );
						solr.addField( SolrFields.POSTCODE_DISTRICT, pcd );
						String location = pcg.getLatLogForPostcodeDistrict( pcd );
						if( location != null )
							solr.addField( SolrFields.LOCATIONS, location );
					}

					// TODO Named entity extraction

					/* ---------------------------------------------------------- */

					// Canonicalize the text - strip newlines etc.
					Pattern whitespace = Pattern.compile( "\\s+" );
					Matcher matcher = whitespace.matcher( text );
					text = matcher.replaceAll( " " ).toLowerCase().trim();

					/* ---------------------------------------------------------- */

					// Add SSDeep hash for the text, to spot similar texts.
					SSDeep ssd = new SSDeep();
					FuzzyHash tfh = ssd.fuzzy_hash_buf( text.getBytes( "UTF-8" ) );
					solr.addField( SolrFields.SSDEEP_PREFIX + tfh.getBlocksize(), tfh.getHash() );
					solr.addField( SolrFields.SSDEEP_PREFIX + ( tfh.getBlocksize() * 2 ), tfh.getHash2() );
					solr.addField( SolrFields.SSDEEP_NGRAM_PREFIX + tfh.getBlocksize(), tfh.getHash() );
					solr.addField( SolrFields.SSDEEP_NGRAM_PREFIX + ( tfh.getBlocksize() * 2 ), tfh.getHash2() );

				}
			}

			// TODO ACT/WctEnricher, currently invoked in the reduce stage to lower query hits, but should shift here.

			// Remove the Text Field if required
			if( !isTextIncluded ) {
				solr.doc.removeField( SolrFields.SOLR_EXTRACTED_TEXT );
			}
		}
		return solr;
	}

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
				if( h.getName().equals( HttpHeaders.CONTENT_TYPE ) && solr.doc.getField( SolrFields.CONTENT_TYPE_SERVED ) == null )
					solr.addField( SolrFields.CONTENT_TYPE_SERVED, h.getValue() );
				// Also, grab the X-Powered-By or Server headers if present:
				if( h.getName().equals( "X-Powered-By" ) )
					solr.addField( SolrFields.GENERATOR, h.getValue() );
				if( h.getName().equals( HttpHeaders.SERVER ) )
					solr.addField( SolrFields.SERVER, h.getValue() );
			}
		} catch( NumberFormatException e ) {
			log.error( "Exception when parsing status code: " + statusCode + ": " + e );
			solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing statusCode: " + e.getMessage() );
		} catch( Exception e ) {
			log.error( "Exception when parsing headers: " + e );
			solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + ": " + e.getMessage() );
		}
	}

	/**
	 * 
	 * @param fullUrl
	 * @return
	 */
	protected static String parseExtension( String fullUrl ) {
		if( fullUrl.lastIndexOf( "/" ) != -1 ) {
			String path = fullUrl.substring( fullUrl.lastIndexOf( "/" ) );
			if( path.indexOf( "?" ) != -1 ) {
				path = path.substring( 0, path.indexOf( "?" ) );
			}
			if( path.indexOf( "&" ) != -1 ) {
				path = path.substring( 0, path.indexOf( "&" ) );
			}
			if( path.indexOf( "." ) != -1 ) {
				String ext = path.substring( path.lastIndexOf( "." ) );
				ext = ext.toLowerCase();
				// Avoid odd/malformed extensions:
				// if( ext.contains("%") )
				// ext = ext.substring(0, path.indexOf("%"));
				ext = ext.replaceAll( "[^0-9a-z]", "" );
				return ext;
			}
		}
		return null;
	}

	/**
	 * Timestamp parsing, for the Crawl Date.
	 */

	private static SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
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
		return formatter.format( getWaybackDate( waybackDate ) );
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
	 * @param serverType
	 */
	private void processContentType( SolrRecord solr, ArchiveRecordHeader header ) {
		// Get the current content-type:
		String contentType = ( String ) solr.doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE );
		String serverType = ( String ) solr.doc.getFieldValue( SolrFields.CONTENT_TYPE_SERVED );

		// Store the raw content type from Tika:
		solr.doc.setField( SolrFields.CONTENT_TYPE_TIKA, contentType );

		// Also get the other content types:
		MediaType mt_tika = MediaType.parse( contentType );
		if( solr.doc.get( SolrFields.CONTENT_TYPE_DROID ) != null ) {
			MediaType mt_droid = MediaType.parse( ( String ) solr.doc.get( SolrFields.CONTENT_TYPE_DROID ).getFirstValue() );
			if( mt_tika == null || mt_tika.equals( MediaType.OCTET_STREAM ) ) {
				contentType = mt_droid.toString();
			} else if( mt_droid.getBaseType().equals( mt_tika.getBaseType() ) && mt_droid.getParameters().get( "version" ) != null ) {
				// Union of results:
				mt_tika = new MediaType( mt_tika, mt_droid.getParameters() );
				contentType = mt_tika.toString();
			}
			if( mt_droid.getParameters().get( "version" ) != null ) {
				solr.doc.addField( SolrFields.CONTENT_VERSION, mt_droid.getParameters().get( "version" ) );
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
			solr.doc.setField( SolrFields.FULL_CONTENT_TYPE, contentType );

		// Fall back on serverType for plain text:
		if( contentType != null && contentType.startsWith( "text/plain" ) ) {
			if( serverType != null ) {
				contentType = serverType;
			}
		}

		// Content-Type can still be null
		if( contentType != null ) {
			// Strip parameters out of main type field:
			solr.doc.setField( SolrFields.SOLR_CONTENT_TYPE, contentType.replaceAll( ";.*$", "" ) );

			// Also add a more general, simplified type, as appropriate:
			// FIXME clean up this messy code:
			if( contentType.matches( "^image/.*$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "image" );
			} else if( contentType.matches( "^(audio|video)/.*$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "media" );
			} else if( contentType.matches( "^text/htm.*$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "html" );
			} else if( contentType.matches( "^application/pdf.*$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "pdf" );
			} else if( contentType.matches( "^.*word$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "word" );
			} else if( contentType.matches( "^.*excel$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "excel" );
			} else if( contentType.matches( "^.*powerpoint$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "powerpoint" );
			} else if( contentType.matches( "^text/plain.*$" ) ) {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "text" );
			} else {
				solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "other" );
			}

			// Remove text from JavaScript, CSS, ...
			if( contentType.startsWith( "application/javascript" ) || contentType.startsWith( "text/javascript" ) || contentType.startsWith( "text/css" ) ) {
				solr.doc.removeField( SolrFields.SOLR_EXTRACTED_TEXT );
			}
		}
	}

	private boolean checkUrl( String url ) {
		for( String exclude : url_excludes ) {
			if( !"".equals( exclude ) && url.matches( ".*" + exclude + ".*" ) ) {
				return false;
			}
		}
		return true;
	}

	private boolean checkProtocol( String url ) {
		for( String include : protocol_includes ) {
			if( "".equals( include ) || url.startsWith( include ) ) {
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
			if( "".equals( include ) || statusCode.startsWith( include ) ) {
				return true;
			}
		}
		// Exclude
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

	private class ParseRunner implements Runnable {
		AbstractParser parser;
		Metadata metadata;
		InputStream input;
		private SolrRecord solr;

		public ParseRunner( AbstractParser parser, InputStream tikainput, Metadata metadata, SolrRecord solr ) {
			this.parser = parser;
			this.metadata = metadata;
			this.input = tikainput;
			this.solr = solr;
		}

		@Override
		public void run() {
			try {
				parser.parse( input, null, metadata, null );
			} catch( Exception e ) {
				log.error( parser.getClass().getName()+".parse(): " + e.getMessage() );
				// Also record as a Solr PARSE_ERROR
				solr.addField( SolrFields.PARSE_ERROR, e.getClass().getName() + " when parsing with "+parser.getClass().getName()+": " + e.getMessage() );
			}
		}
	}
}
