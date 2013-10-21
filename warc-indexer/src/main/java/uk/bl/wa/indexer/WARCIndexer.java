package uk.bl.wa.indexer;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.RESPONSE;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.archive.util.ArchiveUtils;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import uk.bl.wa.extract.LanguageDetector;
import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.nanite.droid.DroidDetector;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.sentimentalj.Sentiment;
import uk.bl.wa.sentimentalj.SentimentalJ;
import uk.bl.wa.util.PostcodeGeomapper;
import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrWebServer;
import uk.bl.wa.util.solr.TikaExtractor;
import uk.bl.wa.util.solr.WritableSolrRecord;
import uk.gov.nationalarchives.droid.command.action.CommandExecutionException;
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
	
	private static Log log = LogFactory.getLog(WARCIndexer.class);
	
	private static final String CLI_USAGE = "[-o <output dir>] [-s <Solr instance>] [-t] [WARC File List]";
	private static final String CLI_HEADER = "WARCIndexer - Extracts metadata and text from Archive Records";
	private static final String CLI_FOOTER = "";
	private static final long BUFFER_SIZE = 1024*1024l; // 10485760 bytes = 10MB.
	private static final Pattern postcodePattern = Pattern.compile("[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}");	
	
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
	private boolean extractElementsUsed = true;
	
	/** */
	private SentimentalJ sentij = new SentimentalJ();

	/** */
	private PostcodeGeomapper pcg = new PostcodeGeomapper();

	/** 
	 * Default constructor, with empty configuration.
	 */
	public WARCIndexer() throws NoSuchAlgorithmException {
		this( ConfigFactory.parseString( ConfigFactory.load().root().render(ConfigRenderOptions.concise()) ) );
	}

	/** 
	 * Preferred constructor, allows passing in configuration from execution environment.
	 */
	public WARCIndexer( Config conf ) throws NoSuchAlgorithmException {
		// Optional configurations:
		this.extractLinks                 = conf.getBoolean("warc.index.extract.linked.resources" );
		this.extractLinkHosts             = conf.getBoolean("warc.index.extract.linked.hosts" );
		this.extractLinkDomains           = conf.getBoolean("warc.index.extract.linked.domains" );
		this.runDroid                     = conf.getBoolean("warc.index.id.droid.enabled" );
		this.passUriToFormatTools         = conf.getBoolean("warc.index.id.useResourceURI");
		this.droidUseBinarySignaturesOnly = conf.getBoolean("warc.index.id.droid.useBinarySignaturesOnly" );
		
		// Instanciate required helpers:
		md5 = MessageDigest.getInstance( "MD5" );
		// Attempt to set up Droid:
		try {
			dd = new DroidDetector();
			dd.setBinarySignaturesOnly(droidUseBinarySignaturesOnly);
			dd.setMaxBytesToScan(BUFFER_SIZE);
		} catch (CommandExecutionException e) {
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
	public WritableSolrRecord extract( String archiveName, ArchiveRecord record ) throws IOException {
		return extract(archiveName, record, true);
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
	public WritableSolrRecord extract( String archiveName, ArchiveRecord record, boolean isTextIncluded ) throws IOException {
		ArchiveRecordHeader header = record.getHeader();
		WritableSolrRecord solr = new WritableSolrRecord();
		
		if( !header.getHeaderFields().isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) && !header.getHeaderValue( HEADER_KEY_TYPE ).equals( RESPONSE ) ) {
				return null;
			}
			
			if( header.getUrl() == null ) return null;
			
			// Check the record type:
			log.info("WARC record "+header.getHeaderValue(WARCConstants.HEADER_KEY_ID)+" type: " + header.getHeaderValue( WARCConstants.HEADER_KEY_TYPE ) );			
			// FIXME This is a duplicate of logic in the WARC record readers, but it should really be here:
			if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) && !header.getHeaderValue( HEADER_KEY_TYPE ).equals( RESPONSE ) ) {
				return null;
			}
			
			for( String h : header.getHeaderFields().keySet()) {
				log.debug("ArchiveHeader: "+h+" -> "+header.getHeaderValue(h));
			}
						
			if( ! record.hasContentHeaders() ) return null;
			
			// Basic headers
			
			// Dates
			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );
			solr.doc.setField( SolrFields.WAYBACK_DATE, waybackDate );
			solr.doc.setField( SolrFields.CRAWL_YEAR,  extractYear(header.getDate()) );
			solr.doc.setField(SolrFields.CRAWL_DATE, parseCrawlDate(waybackDate));			
			
			// 
			byte[] md5digest = md5.digest( header.getUrl().getBytes( "UTF-8" ) );
			String md5hex = new String( Base64.encodeBase64( md5digest ) );
			solr.doc.setField( SolrFields.ID, waybackDate + "/" + md5hex);
			solr.doc.setField( SolrFields.ID_LONG, Long.parseLong(waybackDate + "00") + ( (md5digest[1] << 8) + md5digest[0] ) );
			solr.doc.setField( SolrFields.SOLR_DIGEST, header.getHeaderValue(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST) );
			solr.doc.setField( SolrFields.SOLR_URL, header.getUrl() );
			String fullUrl = header.getUrl();
			// Also pull out the file extension, if any:
			solr.doc.addField(SolrFields.CONTENT_TYPE_EXT, parseExtension(fullUrl) );
			// Strip down very long URLs to avoid "org.apache.commons.httpclient.URIException: Created (escaped) uuri > 2083"
			if( fullUrl.length() > 2000 ) fullUrl = fullUrl.substring(0, 2000);
			String[] urlParts = canon.urlStringToKey( fullUrl ).split( "/" );
			// Spot 'slash pages':
			if( urlParts.length == 1 || (urlParts.length >= 2 && urlParts[1].matches("^index\\.[a-z]+$") ) ) 
				solr.doc.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_SLASHPAGE );
			// Spot 'robots.txt':
			if( urlParts.length >= 2 && urlParts[1].equalsIgnoreCase("robots.txt") ) 
				solr.doc.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_ROBOTS_TXT );
			// Record the domain (strictly, the host): 
			String host = urlParts[ 0 ];
			solr.doc.setField( SolrFields.SOLR_HOST, host );
			solr.doc.setField( SolrFields.DOMAIN, LinkExtractor.extractPrivateSuffixFromHost(host) );
			solr.doc.setField( SolrFields.PUBLIC_SUFFIX, LinkExtractor.extractPublicSuffixFromHost(host) );

			// Parse HTTP headers:
			InputStream tikainput = null;
			String statusCode = null;
			if( record instanceof WARCRecord ) {
				// There are not always headers! The code should check first.
				String statusLine = HttpParser.readLine(record, "UTF-8");
				if( statusLine != null ) {
					String firstLine[] = statusLine.split(" ");
					if( firstLine.length > 1 ) {
						statusCode = firstLine[1].trim();
						this.processHeaders(solr, statusCode, HttpParser.parseHeaders(record, "UTF-8"));
					} else {
						log.warn("Could not parse status line: "+statusLine);
					}
				}
				// No need for this, as the headers have already been read from the InputStream (above):
				// WARCRecordUtils.getPayload(record);
				tikainput = record;
			} else if ( record instanceof ARCRecord ) {
				ARCRecord arcr = (ARCRecord) record;
				statusCode = ""+arcr.getStatusCode();
				this.processHeaders(solr, statusCode, arcr.getHttpHeaders());
				arcr.skipHttpHeader();
				tikainput = arcr;
			} else {
				log.error("FAIL! Unsupported archive record type.");
				return solr;
			}
			
			// Skip recording non-content URLs (i.e. 2xx responses only please):
			if( statusCode == null || !statusCode.startsWith("2") ) {
				log.error("Skipping this record as statusCode != 2xx: "+header.getUrl());
				return null;
			}
			
			// -----------------------------------------------------
			// Parse payload using Tika: 
			// -----------------------------------------------------
			
			// Mark the start of the payload, and then run Tika on it:
			tikainput = new BufferedInputStream( new BoundedInputStream( tikainput, BUFFER_SIZE ), (int) BUFFER_SIZE );
			tikainput.mark((int) header.getLength());
			if( passUriToFormatTools ) {
				solr = tika.extract( solr, tikainput, header.getUrl() );
			} else {
				solr = tika.extract( solr, tikainput, null );
			}
						
			// Pull out the first few bytes, to hunt for new format by magic:
			int MAX_FIRST_BYTES = 32;
			try {
				tikainput.reset();
				byte[] ffb = new byte[MAX_FIRST_BYTES];
				int read = tikainput.read(ffb);
				if( read >= 4 ) {
					String hexBytes = Hex.encodeHexString(ffb);
					solr.addField(SolrFields.CONTENT_FFB, hexBytes.substring(0,2*4));
					StringBuilder separatedHexBytes = new StringBuilder();
					for( String hexByte : Splitter.fixedLength(2).split(hexBytes) ) {
						separatedHexBytes.append(hexByte);
						separatedHexBytes.append(" ");
					}
					solr.addField(SolrFields.CONTENT_FIRST_BYTES, separatedHexBytes.toString().trim());
				}
			} catch( IOException i ) {
				log.error( i + ": " +i.getMessage() + ";ffb; " + header.getUrl() + "@" + header.getOffset() );
			}
			
			// Also run DROID (restricted range):
			if( dd != null && runDroid == true ) {
				try {
					tikainput.reset();
					// Pass the URL in so DROID can fall back on that:
					Metadata metadata = new Metadata();
					if( passUriToFormatTools ) {
						UURI uuri = UURIFactory.getInstance(fullUrl);
						// Droid seems unhappy about spaces in filenames, so hack to avoid:
						String cleanUrl = uuri.getName().replace(" ", "+");
						metadata.set(Metadata.RESOURCE_NAME_KEY, cleanUrl);
					}
					// Run Droid:
					MediaType mt = dd.detect(tikainput, metadata);
					solr.addField( SolrFields.CONTENT_TYPE_DROID, mt.toString() );
				} catch( Exception i ) {
					// Note that DROID complains about some URLs with an IllegalArgumentException.
					log.error( i + ": " +i.getMessage() + ";dd; " + fullUrl + " @" + header.getOffset() );
				}
			}

			// Derive normalised/simplified content type:
			processContentType(solr, header);
			
			// Pass on to other extractors as required, resetting the stream before each:
			// Entropy, compressibility, fussy hashes, etc.
			Metadata metadata = new Metadata();
			try {
				tikainput.reset();
				// JSoup link extractor for (x)html, deposit in 'links' field.
				String mime = ( String ) solr.doc.getField( SolrFields.SOLR_CONTENT_TYPE ).getValue();
				HashMap<String,String> hosts = new HashMap<String,String>();
				HashMap<String,String> suffixes = new HashMap<String,String>();
				HashMap<String,String> domains = new HashMap<String,String>();
				if( mime.startsWith( "text" ) ) {
					// JSoup NEEDS the URL to function:
					metadata.set(Metadata.RESOURCE_NAME_KEY, header.getUrl());
					ParseRunner parser = new ParseRunner( hfp, tikainput, metadata );
					Thread thread = new Thread( parser, Long.toString( System.currentTimeMillis() ) );
					try {
						thread.start();
						thread.join( 30000L );
						thread.interrupt();
					} catch( Exception e ) {
						log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
					}

					// Process links:
					String links_list = metadata.get(HtmlFeatureParser.LINK_LIST);
					if( links_list != null ) {
						String lhost, ldomain, lsuffix;
						for( String link : links_list.split(" ") ) {
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
						String de = metadata.get(HtmlFeatureParser.DISTINCT_ELEMENTS);
						if( de != null ) {
							for( String e : de.split(" ") ) {
								solr.addField(SolrFields.ELEMENTS_USED, e);
							}
						}
					}
					for( String lurl : metadata.getValues(Metadata.LICENSE_URL)) {
						solr.addField(SolrFields.LICENSE_URL, lurl);
					}
					
				} else if( mime.startsWith("image") ) {
					// TODO Extract image properties.

				} else if( mime.startsWith("application/pdf") ) {
					// TODO e.g. Use PDFBox Preflight to analyse? https://pdfbox.apache.org/userguide/preflight.html

				}
			} catch( Exception i ) {
				log.error( i + ": " +i.getMessage() + ";x; " + header.getUrl() + "@" + header.getOffset() );
			}
			
			// --- The following extractors don't need to re-read the payload ---
			
			// Pull out the text:
			if (solr.doc.getField(SolrFields.SOLR_EXTRACTED_TEXT) != null) {
				String text = (String) solr.doc.getField(
						SolrFields.SOLR_EXTRACTED_TEXT).getFirstValue();
				text = text.trim();
				if (! "".equals(text) ) {
					/* ---------------------------------------------------------- */
					
					if( metadata.get(Metadata.CONTENT_LANGUAGE) == null ) {
						String li = ld.detectLanguage(text);
						if( li != null )
							solr.addField(SolrFields.CONTENT_LANGUAGE, li);
					} else {
						solr.addField(SolrFields.CONTENT_LANGUAGE, metadata.get(Metadata.CONTENT_LANGUAGE) );
					}
					
					/* ---------------------------------------------------------- */
					
					// Sentiment Analysis:
					int sentilen = 10000;
					if (sentilen > text.length())
						sentilen = text.length();
					String sentitext = text.substring(0, sentilen);
					//	metadata.get(HtmlFeatureParser.FIRST_PARAGRAPH);
					
					Sentiment senti = sentij.analyze( sentitext );
					double sentilog = Math.signum( senti.getComparative() ) * 
							(Math.log(1.0+Math.abs( senti.getComparative() ))/40.0 );
					int sentii = (int) (SolrFields.SENTIMENTS.length * ( 0.5 + sentilog  ));
					if( sentii < 0 ) {
						log.debug("Caught a sentiment rating less than zero: "+sentii+" from "+sentilog);
						sentii = 0;
					}
					if( sentii >= SolrFields.SENTIMENTS.length ) {
						log.debug("Caught a sentiment rating too large to be in range: "+sentii+" from "+sentilog);
						sentii = SolrFields.SENTIMENTS.length - 1;
					}
					//if( sentii != 3 )
					//	log.debug("Got sentiment: " + sentii+" "+sentilog+" "+ SolrFields.SENTIMENTS[sentii] );
					// Map to sentiment scale:
					solr.addField(SolrFields.SENTIMENT, SolrFields.SENTIMENTS[sentii]);
					solr.addField(SolrFields.SENTIMENT_SCORE, ""+senti.getComparative());
					
					/* ---------------------------------------------------------- */

					// Postcode Extractor (based on text extracted by Tika)
					Matcher pcm = postcodePattern.matcher(text);
					Set<String> pcs = new HashSet<String>();
					while (pcm.find()) pcs.add(pcm.group());
					for( String pc : pcs ) {
						solr.addField(SolrFields.POSTCODE, pc);
						String pcd = pc.substring(0, pc.lastIndexOf(" "));
						solr.addField(SolrFields.POSTCODE_DISTRICT, pcd);
						String location = pcg.getLatLogForPostcodeDistrict(pcd);
						if( location != null ) 
							solr.addField(SolrFields.LOCATIONS, location);
					}
					
					// TODO Named entity extraction

					/* ---------------------------------------------------------- */
					
					// Canonicalize the text - strip newlines etc.
					Pattern whitespace = Pattern.compile("\\s+");
					Matcher matcher = whitespace.matcher(text);
					text = matcher.replaceAll(" ").toLowerCase().trim();
					
					/* ---------------------------------------------------------- */

					// Add SSDeep hash for the text, to spot similar texts.
					SSDeep ssd = new SSDeep();
					FuzzyHash tfh = ssd.fuzzy_hash_buf(text.getBytes("UTF-8"));
					solr.addField( SolrFields.SSDEEP_PREFIX+tfh.getBlocksize(), tfh.getHash() );
					solr.addField( SolrFields.SSDEEP_PREFIX+(tfh.getBlocksize()*2), tfh.getHash2() );
					solr.addField( SolrFields.SSDEEP_NGRAM_PREFIX+tfh.getBlocksize(), tfh.getHash() );
					solr.addField( SolrFields.SSDEEP_NGRAM_PREFIX+(tfh.getBlocksize()*2), tfh.getHash2() );

				}
			}
			
			// TODO ACT/WctEnricher, currently invoked in the reduce stage to lower query hits.
			
			// Remove the Text Field if required
			if( !isTextIncluded){ 
				solr.doc.removeField(SolrFields.SOLR_EXTRACTED_TEXT);
			}

		}
		return solr;
	}
	
	/* ----------------------------------- */
	
	private void processHeaders(WritableSolrRecord solr, String statusCode, Header[] httpHeaders) {
		try {
			// This is a simple test that the status code setting worked:
			int statusCodeInt = Integer.parseInt(statusCode);
			if( statusCodeInt < 0 || statusCodeInt > 1000 ) 
				throw new Exception("Status code out of range: "+statusCodeInt); 
			// Get the other headers:
			for( Header h : httpHeaders ) {
				// Get the type from the server
				if( h.getName().equals(HttpHeaders.CONTENT_TYPE)) 
					solr.addField(SolrFields.CONTENT_TYPE_SERVED, h.getValue());
				// Also, grab the X-Powered-By or Server headers if present:
				if( h.getName().equals("X-Powered-By") ) 
					solr.addField(SolrFields.GENERATOR, h.getValue() );
				if( h.getName().equals(HttpHeaders.SERVER) ) 
					solr.addField(SolrFields.SERVER, h.getValue() );
			}
		} catch( NumberFormatException e ) {
			log.error("Exception when parsing status code: "+statusCode+": "+e);
			solr.addField(SolrFields.PARSE_ERROR, e.getClass().getName()+" when parsing statusCode: "+e.getMessage());
		} catch( Exception e ) {
			log.error("Exception when parsing headers: "+e);
			solr.addField(SolrFields.PARSE_ERROR, e.getClass().getName()+": "+e.getMessage());
		}
	}

	/**
	 * 
	 * @param fullUrl
	 * @return
	 */
	protected static String parseExtension(String fullUrl) {
		if( fullUrl.lastIndexOf("/") != -1 ) {
			String path = fullUrl.substring(fullUrl.lastIndexOf("/"));
			if( path.indexOf("?") != -1 ) {
				path = path.substring(0, path.indexOf("?"));
			}
			if( path.indexOf("&") != -1 ) {
				path = path.substring(0, path.indexOf("&"));
			}				
			if( path.indexOf(".") != -1 ) {
				String ext = path.substring(path.lastIndexOf("."));
				ext = ext.toLowerCase();
				// Avoid odd/malformed extensions:
				//if( ext.contains("%") )
				//	ext = ext.substring(0, path.indexOf("%"));
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
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/**
	 * 
	 * @param waybackDate
	 * @return
	 */
	protected static String parseCrawlDate(String waybackDate) {
		try {
			// Some ARC records have 12-, 16- or 17-digit dates
			if( waybackDate.length() == 12 ) {
				return formatter.format( ArchiveUtils.parse12DigitDate( waybackDate ) );
			} else if( waybackDate.length() == 14 ) {
				return formatter.format( ArchiveUtils.parse14DigitDate( waybackDate ) );
			} else if( waybackDate.length() == 16 ) {
				return formatter.format( ArchiveUtils.parse17DigitDate( waybackDate+"0" ) );
			} else if( waybackDate.length() >= 17 ) {
				return formatter.format( ArchiveUtils.parse17DigitDate( waybackDate.substring( 0, 17 ) ) );
			}
		} catch( ParseException p ) {
			p.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param timestamp
	 * @return
	 */
	public static String extractYear(String timestamp) {
		// Default to 'unknown':
		String waybackYear = "unknown";
		String waybackDate = timestamp.replaceAll( "[^0-9]", "" );
		if( waybackDate != null ) 
			waybackYear = waybackDate.substring(0,4);
		// Reject bad values by resetting to 'unknown':
		if( "0000".equals(waybackYear)) waybackYear="unknown";
		// Return
		return waybackYear;
	}
	
	/**
	 * 
	 * @param solr
	 * @param header
	 * @param serverType
	 */
	private void processContentType( WritableSolrRecord solr, ArchiveRecordHeader header) {
		// Get the current content-type:
		String contentType =  (String) solr.doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE );
		String serverType =  (String) solr.doc.getFieldValue( SolrFields.CONTENT_TYPE_SERVED );
		
		// Store the raw content type from Tika:
		solr.doc.setField( SolrFields.CONTENT_TYPE_TIKA, contentType );
		
		// Also get the other content types:
		MediaType mt_tika = MediaType.parse(contentType);
		if( solr.doc.get(SolrFields.CONTENT_TYPE_DROID) != null ) {
			MediaType mt_droid = MediaType.parse((String)solr.doc.get(SolrFields.CONTENT_TYPE_DROID).getFirstValue());
			if( mt_tika == null || mt_tika.equals(MediaType.OCTET_STREAM ) ) {
				contentType = mt_droid.toString();
			} else if( mt_droid.getBaseType().equals(mt_tika.getBaseType()) 
					&& mt_droid.getParameters().get("version") != null ) {
				// Union of results:
				mt_tika = new MediaType(mt_tika, mt_droid.getParameters());	
				contentType = mt_tika.toString();
			}
			if( mt_droid.getParameters().get("version") != null ) {
				solr.doc.addField(SolrFields.CONTENT_VERSION, mt_droid.getParameters().get("version") );
			}
		}

		// Allow header MIME
		if( contentType.isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( "WARC-Identified-Payload-Type" ) ) {
				contentType =  ( ( String ) header.getHeaderFields().get( "WARC-Identified-Payload-Type" ) );
			} else {
				contentType = header.getMimetype();
			}
		}
		
		// Determine content type:
		solr.doc.setField( SolrFields.FULL_CONTENT_TYPE, contentType );
		
		// Fall back on serverType for plain text:
		if( contentType != null && contentType.startsWith("text/plain")){
			if( serverType != null ){
				contentType = serverType;
			}
		}
		
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
		if( contentType.startsWith("application/javascript") || 
				contentType.startsWith("text/javascript") || 
				contentType.startsWith("text/css") ){
			solr.doc.removeField(SolrFields.SOLR_EXTRACTED_TEXT);
		}
	}

	
	/**
	 * 
	 * @param args
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws TransformerException 
	 * @throws TransformerFactoryConfigurationError 
	 * @throws SolrServerException 
	 */
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException, TransformerFactoryConfigurationError, TransformerException, SolrServerException {
		
		CommandLineParser parser = new PosixParser();
		String outputDir = null;
		String solrUrl = null;
		boolean isTextRequired = false;
		boolean slashPages = false;
		
		Options options = new Options();
		options.addOption("o", "output", true, "The directory to contain the output XML files");
		options.addOption("s", "solr", true, "The URL of the required Solr Instance");
		options.addOption("t", "text", false, "Include text in XML in output files");
		options.addOption("r", "slash", false, "Only process slash (root) pages.");
		
		try {
		    // parse the command line arguments
		    CommandLine line = parser.parse( options, args );
		   	String cli_args[] = line.getArgs();
		   
		
		   	// Check that a mandatory Archive file(s) has been supplied
		   	if( !( cli_args.length > 0 ) ) {
				//System.out.println( "Arguments required are 1) Output directory 2) List of WARC files 3) Optionally --update-solr-server=url" );
		   		printUsage(options);
				System.exit( 0 );
		   	}

		   	// Get the output directory, if set
		   	if(line.hasOption("o")){
		   		outputDir = line.getOptionValue("o");
		   		if(outputDir.endsWith("/")||outputDir.endsWith("\\")){
		   			outputDir = outputDir.substring(0, outputDir.length()-1);
		   		}
		
		   		outputDir = outputDir + "//";
		   		System.out.println("Output Directory is: " + outputDir);
		   		File dir = new File(outputDir);
		   		if(!dir.exists()){
		   			FileUtils.forceMkdir(dir);
		   		}
		   	}
		   	
		   	// Get the Solr Url, if set
		   	if(line.hasOption("s")){
		   		solrUrl = line.getOptionValue("s");
		   		if(solrUrl.contains("\"")){
		   			solrUrl  = solrUrl.replaceAll("\"", "");
				}
		   	}
		   	
		   	// Check if the text field is required in the XML output
		   	if(line.hasOption("t") || line.hasOption("s")){
		   		isTextRequired = true;
		   	}

		   	if( line.hasOption( "r" ) ) {
		   		slashPages = true;
		   	}
		   	
		   	// Check that either an output dir or Solr URL is supplied
		   	if(outputDir == null && solrUrl == null){
		   		System.out.println( "A Solr URL or an Output Directory must be supplied" );
		   		printUsage(options);
		   		System.exit( 0 );
		   	}
		   	
		   	// Check that both an output dir and Solr URL are not supplied
		   	if(outputDir != null && solrUrl != null){
		   		System.out.println( "A Solr URL and an Output Directory cannot both be specified" );
		   		printUsage(options);
		   		System.exit( 0 );
		   	}
	   	
		   	parseWarcFiles(outputDir, solrUrl, cli_args, isTextRequired, slashPages);
		
		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	
	}
	
	/**
	 * @param outputDir
	 * @param args
	 * @throws NoSuchAlgorithmException
	 * @throws MalformedURLException
	 * @throws IOException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 */
	public static void parseWarcFiles(String outputDir, String solrUrl, String[] args, boolean isTextRequired, boolean slashPages) throws NoSuchAlgorithmException, MalformedURLException, IOException, TransformerFactoryConfigurationError, TransformerException, SolrServerException{
		
		WARCIndexer windex = new WARCIndexer();
				
		SolrWebServer solrWeb = null;
		
		// If the Solr URL is set initiate a connections
		if(solrUrl != null) {		
			solrWeb = new SolrWebServer(solrUrl);
		}
		
		int totInputFile = args.length;
		int curInputFile = 1;
					
		// Loop through each Warc files
		for( String inputFile : args ) {
								
			System.out.println("Parsing Archive File [" + curInputFile + "/" + totInputFile + "]:" + inputFile);
			File inFile = new File(inputFile);
			String fileName = inFile.getName();
			String outputWarcDir = outputDir + fileName + "//";
			File dir = new File(outputWarcDir);
	   		if(!dir.exists() && solrUrl == null){
	   			FileUtils.forceMkdir(dir);
	   		}
			
			ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
			Iterator<ArchiveRecord> ir = reader.iterator();
			int recordCount = 1;
			
			// Iterate though each record in the WARC file
			while( ir.hasNext() ) {
				ArchiveRecord rec = ir.next();
				WritableSolrRecord doc = windex.extract("",rec, isTextRequired);

				if( doc != null ) {
					File fileOutput = new File(outputWarcDir + "//" + "FILE_" + recordCount + ".xml");
					
					if( !slashPages || ( doc.doc.getFieldValue( SolrFields.SOLR_URL_TYPE ) != null &&
									     doc.doc.getFieldValue( SolrFields.SOLR_URL_TYPE ).equals( SolrFields.SOLR_URL_TYPE_SLASHPAGE ) ) ) {
						// Write XML to file if not posting straight to the server.
						if(solrUrl == null) {
							writeXMLToFile(doc.toXml(), fileOutput);
						}else{
							// Post to Solr
							solrWeb.updateSolrDoc(doc.doc);
						}
						recordCount++;
					}

				}			
			}
			curInputFile++;
		}
		
		// Commit any Solr Updates
		if(solrWeb != null) {		
			solrWeb.commit();
		}
		
		System.out.println("WARC Indexer Finished");
	}
	
	
	public static void prettyPrintXML( String doc ) throws TransformerFactoryConfigurationError, TransformerException {
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		//initialize StreamResult with File object to save to file
		StreamResult result = new StreamResult(new StringWriter());
		StreamSource source = new StreamSource(new StringReader(doc));
		transformer.transform(source, result);
		String xmlString = result.getWriter().toString();
		System.out.println(xmlString);		
	}
	
	/**
	 * @param xml
	 * @param file
	 * @throws IOException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 */
	public static void writeXMLToFile( String xml, File file ) throws IOException, TransformerFactoryConfigurationError, TransformerException {
		
		Result result = new StreamResult(file);
		Source source =  new StreamSource(new StringReader(xml));
		
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		//FileUtils.writeStringToFile(file, xml);
		
		transformer.transform(source, result);
	  
	}
	
	/**
	 * @param options
	 */
	private static void printUsage( Options options ) {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.setWidth( 80 );
		helpFormatter.printHelp( CLI_USAGE, CLI_HEADER, options, CLI_FOOTER );
	}

	private class ParseRunner implements Runnable {
		HtmlFeatureParser hfp;
		Metadata metadata;
		InputStream input;

		public ParseRunner( HtmlFeatureParser parser, InputStream tikainput, Metadata metadata ) {
			this.hfp = parser;
			this.metadata = metadata;
			this.input = tikainput;
		}

		@Override
		public void run() {
			try {
				hfp.parse( input, null, metadata, null );
			} catch( Exception e ) {
				log.error( "HtmlFeatureParser.parse(): " + e.getMessage() );
			}
		}

	}
}
