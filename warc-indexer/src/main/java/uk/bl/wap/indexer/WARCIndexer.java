package uk.bl.wap.indexer;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.RESPONSE;
//import static uk.bl.wap.util.solr.SolrFields.SOLR_LINKS;
import static uk.bl.wap.util.solr.SolrFields.SOLR_LINKS_HOSTS;
import static uk.bl.wap.util.solr.SolrFields.SOLR_LINKS_PUBLIC_SUFFIXES;
import static uk.bl.wap.util.solr.SolrFields.SOLR_CONTENT_TYPE;

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
import java.util.Iterator;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;
import org.archive.util.ArchiveUtils;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

import uk.bl.wa.extract.LanguageDetector;
import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.sentimentalj.Sentiment;
import uk.bl.wa.sentimentalj.SentimentalJ;
import uk.bl.wap.util.solr.SolrFields;
import uk.bl.wap.util.solr.SolrWebServer;
import uk.bl.wap.util.solr.TikaExtractor;
import uk.bl.wap.util.solr.WritableSolrRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexer {
	
	private static Log log = LogFactory.getLog(WARCIndexer.class);
	
	private static final String CLI_USAGE = "[-o <output dir>] [-s <Solr instance>] [-t] [WARC File List]";
	private static final String CLI_HEADER = "WARCIndexer - Extracts metadata and text from Archive Records";
	private static final String CLI_FOOTER = "";
	private static final int BUFFER_SIZE = 104857600;
	private static final Pattern postcodePattern = Pattern.compile("[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}");	
	
	TikaExtractor tika = new TikaExtractor();
	MessageDigest md5 = null;
	AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();
	SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );

	public WARCIndexer() throws NoSuchAlgorithmException {
		md5 = MessageDigest.getInstance( "MD5" );
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
	 * Removes the text field if flag set
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
			
			//for( String h : header.getHeaderFields().keySet()) {
			//	System.out.println("ArchiveHeader: "+h+" -> "+header.getHeaderValue(h));
			//}
			
			if( ! record.hasContentHeaders() ) return null;
			
			// Basic headers
			
			// Dates
			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );
			solr.doc.setField( SolrFields.WAYBACK_DATE, waybackDate );
			String year = extractYear(header.getDate());
			if( !"0000".equals(year))
				solr.doc.setField( SolrFields.CRAWL_YEAR, year );
			try {
				// Some ARC records only have 12-digit dates
				if( waybackDate.length() == 12 ) {
					solr.doc.setField( SolrFields.CRAWL_DATE, formatter.format( ArchiveUtils.parse12DigitDate( waybackDate ) ) );
				} else {
					if( waybackDate.length() == 14 ) {
						solr.doc.setField( SolrFields.CRAWL_DATE, formatter.format( ArchiveUtils.parse14DigitDate( waybackDate ) ) );
					}
				}
			} catch( ParseException p ) {
				p.printStackTrace();
			}
			
			
			// 
			byte[] md5digest = md5.digest( header.getUrl().getBytes( "UTF-8" ) );
			String md5hex = new String( Base64.encodeBase64( md5digest ) );
			solr.doc.setField( SolrFields.SOLR_ID, waybackDate + "/" + md5hex);
			solr.doc.setField( SolrFields.ID_LONG, Long.parseLong(waybackDate + "00") + ( (md5digest[1] << 8) + md5digest[0] ) );
			solr.doc.setField( SolrFields.SOLR_DIGEST, header.getHeaderValue(WARCConstants.HEADER_KEY_PAYLOAD_DIGEST) );
			solr.doc.setField( SolrFields.SOLR_URL, header.getUrl() );
			// Spot 'slash pages':
			String fullUrl = header.getUrl();
			// Also pull out the file extension, if any:
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
					solr.doc.addField(SolrFields.CONTENT_TYPE_EXT, ext);
				}
			}
			// Strip down very long URLs to avoid "org.apache.commons.httpclient.URIException: Created (escaped) uuri > 2083"
			if( fullUrl.length() > 2000 ) fullUrl = fullUrl.substring(0, 2000);
			String[] urlParts = canon.urlStringToKey( fullUrl ).split( "/" );
			if( urlParts.length == 1 || (urlParts.length == 2 && urlParts[1].matches("^index\\.[a-z]+$") ) ) 
				solr.doc.setField( SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_SLASHPAGE );
			// Record the domain (strictly, the host): 
			String domain = urlParts[ 0 ];
			solr.doc.setField( SolrFields.SOLR_DOMAIN, domain );
			solr.doc.setField( SolrFields.PRIVATE_SUFFIX, LinkExtractor.extractPrivateSuffixFromHost(domain) );
			solr.doc.setField( SolrFields.PUBLIC_SUFFIX, LinkExtractor.extractPublicSuffixFromHost(domain) );
			// TOOD Add Private Suffix?

			// Parse HTTP headers:
			// TODO Add X-Powered-By, Server as generators? Maybe Server as served_by? Or just server?
			String referrer = null;
			InputStream tikainput = null;
			String statusCode = null;
			String serverType = null;
			if( record instanceof WARCRecord ) {
				String firstLine[] = HttpParser.readLine(record, "UTF-8").split(" ");
				statusCode = firstLine[1].trim();
				Header[] headers = HttpParser.parseHeaders(record, "UTF-8");
				for( Header h : headers ) {
					//System.out.println("HttpHeader: "+h.getName()+" -> "+h.getValue());
					// FIXME This can't work, because the Referer is in the Request, not the Response.
					// TODO Generally, need to think about ensuring the request and response are brought together.
					if( h.getName().equals(HttpHeaders.REFERER))
						referrer = h.getValue();
					if( h.getName().equals(HttpHeaders.CONTENT_TYPE))
						serverType = h.getValue();
				}
				// No need for this, as the headers have already been read from the InputStream:
				// WARCRecordUtils.getPayload(record);
				tikainput = record;
			
			} else if ( record instanceof ARCRecord ) {
				ARCRecord arcr = (ARCRecord) record;
				statusCode = ""+arcr.getStatusCode();
				for( Header h : arcr.getHttpHeaders() ) {
					//System.out.println("HttpHeader: "+h.getName()+" -> "+h.getValue());
					if( h.getName().equals(HttpHeaders.REFERER))
						referrer = h.getValue();
					if( h.getName().equals(HttpHeaders.CONTENT_TYPE))
						serverType = h.getValue();
				}
				arcr.skipHttpHeader();
				tikainput = arcr;
			} else {
				System.err.println("FAIL! Unsupported archive record type.");
				return solr;
			}
			
			// Fields from Http headers:
			solr.addField( SolrFields.SOLR_REFERRER_URI, referrer );
			// Get the type from the server
			solr.addField(SolrFields.CONTENT_TYPE_SERVED, serverType);			
			
			// Skip recording non-content URLs (i.e. 2xx responses only please):
			if( statusCode == null || !statusCode.startsWith("2") ) return null;
			
			// -----------------------------------------------------
			// Parse payload using Tika: 
			// -----------------------------------------------------
			
			// Mark the start of the payload, and then run Tika on it:
			tikainput = new BufferedInputStream( tikainput, BUFFER_SIZE );
			tikainput.mark((int) header.getLength());
			solr = tika.extract( solr, tikainput, header.getUrl() );
			// Derive normalised/simplified content type:
			processContentType(solr, header, serverType);
						
			// Pull out the first four bytes, to hunt for new format by magic:
			try {
				tikainput.reset();
				byte[] ffb = new byte[4];
				int read = tikainput.read(ffb);
				if( read == 4 ) {
					solr.addField(SolrFields.CONTENT_FFB, Hex.encodeHexString(ffb));
				}
			} catch( IOException i ) {
				System.err.println( i.getMessage() + "; " + header.getUrl() + "@" + header.getOffset() );
			}

			
			// Pass on to other extractors as required, resetting the stream before each:
			// Entropy, compressibility, fussy hashes, etc.
			Metadata metadata = new Metadata();
			try {
				tikainput.reset();
				// JSoup link extractor for (x)html, deposit in 'links' field.
				String mime = ( String ) solr.doc.getField( SOLR_CONTENT_TYPE ).getValue();
				HashMap<String,String> hosts = new HashMap<String,String>();
				HashMap<String,String> suffixes = new HashMap<String,String>();
				HashMap<String,String> domains = new HashMap<String,String>();
				if( mime.startsWith( "text" ) ) {
					HtmlFeatureParser hfp = new HtmlFeatureParser();
					metadata.set(Metadata.RESOURCE_NAME_KEY, header.getUrl());
					hfp.parse(tikainput, null, metadata, null);
					String links_list = metadata.get(HtmlFeatureParser.LINK_LIST);
					if( links_list != null ) {
						String host, suffix;
						for( String link : links_list.split(" ") ) {
							host = LinkExtractor.extractHost( link );
							if( !host.equals( LinkExtractor.MALFORMED_HOST ) ) {
								hosts.put( host, "" );
							}
							suffix = LinkExtractor.extractPublicSuffix( link );
							if( suffix != null ) {
								suffixes.put( suffix, "" );
							}
							domain = LinkExtractor.extractPrivateSuffix( link );
							if( domain != null ) {
								domains.put( domain, "" );
							}
						}
						Iterator<String> iterator = hosts.keySet().iterator();
						while( iterator.hasNext() ) {
							solr.addField( SOLR_LINKS_HOSTS, iterator.next() );
						}
						iterator = suffixes.keySet().iterator();
						while( iterator.hasNext() ) {
							solr.addField( SOLR_LINKS_PUBLIC_SUFFIXES, iterator.next() );
						}
						iterator = domains.keySet().iterator();
						while( iterator.hasNext() ) {
							solr.addField( SolrFields.SOLR_LINKS_PRIVATE_SUFFIXES, iterator.next() );
						}
					}
				} else if( mime.startsWith("image") ) {
					// TODO Extract image properties.

				} else if( mime.startsWith("application/pdf") ) {
					// TODO e.g. Use PDFBox Preflight to analyse? https://pdfbox.apache.org/userguide/preflight.html

				}
			} catch( Exception i ) {
				System.err.println( i.getMessage() + "; " + header.getUrl() + "@" + header.getOffset() );
			}
			
			// --- The following extractors don't need to re-read the payload ---
			
			// Pull out the text:
			if (solr.doc.getField(SolrFields.SOLR_EXTRACTED_TEXT) != null) {
				String text = (String) solr.doc.getField(
						SolrFields.SOLR_EXTRACTED_TEXT).getFirstValue();
				text = text.trim();
				if (! "".equals(text) ) {
					LanguageDetector ld = new LanguageDetector();
					String li = ld.detectLanguage(text);
					if( li != null )
					  solr.addField(SolrFields.CONTENT_LANGUAGE, li);
					
					// Sentiment Analysis:
					int sentilen = 5000;
					if (sentilen > text.length())
						sentilen = text.length();
					String sentitext = text.substring(0, sentilen);
					//	metadata.get(HtmlFeatureParser.FIRST_PARAGRAPH);
						
					SentimentalJ sentij = new SentimentalJ();
					Sentiment senti = sentij.analyze( sentitext );
					int sentii = (int) (SolrFields.SENTIMENTS.length * ((senti.getComparative()+2.0)/4.0));
					if( sentii < 0 ) {
						log.warn("Caught a sentiment rating less than zero: "+sentii+" built from "+senti.getComparative());
						sentii = 0;
					}
					if( sentii >= SolrFields.SENTIMENTS.length ) {
						log.warn("Caught a sentiment rating too large to be in range: "+sentii+" built from "+senti.getComparative());
						sentii = SolrFields.SENTIMENTS.length - 1;
					}
					//log.debug("Got sentiment: " + sentii+" "+ SolrFields.SENTIMENTS[sentii] + " from " +text.substring(0, sentilen) );
					// Map to sentiment scale:
					solr.addField(SolrFields.SENTIMENT, SolrFields.SENTIMENTS[sentii]);

					// Postcode Extractor (based on text extracted by Tika)
					Matcher pcm = postcodePattern.matcher(text);
					while (pcm.find()) {
						String pc = pcm.group();
					//	log.info("Got Postcode: " + pc);
						solr.addField(SolrFields.POSTCODE, pc);
						solr.addField(SolrFields.POSTCODE_DISTRICT, pc.substring(0, pc.lastIndexOf(" ")));
					}

					// TODO Named entity extraction

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
	
	/**
	 * 
	 * @param timestamp
	 * @return
	 */
	public static String extractYear(String timestamp) {
		String waybackYear = "unknown-year";
		String waybackDate = timestamp.replaceAll( "[^0-9]", "" );
		if( waybackDate != null ) 
			waybackYear = waybackDate.substring(0,4);
		return waybackYear;
	}
	
	private void processContentType( WritableSolrRecord solr, ArchiveRecordHeader header, String serverType) {
		StringBuilder mime = new StringBuilder();
		mime.append( ( ( String ) solr.doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE ) ) );
		if( mime.toString().isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( "WARC-Identified-Payload-Type" ) ) {
				mime.append( ( ( String ) header.getHeaderFields().get( "WARC-Identified-Payload-Type" ) ) );
			} else {
				mime.append( header.getMimetype() );
			}
		}
		
		// Determine content type:
		String contentType = mime.toString();
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
		if( mime.toString().matches( "^image/.*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "image" );
		} else if( mime.toString().matches( "^(audio|video)/.*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "media" );
		} else if( mime.toString().matches( "^text/htm.*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "html" );
		} else if( mime.toString().matches( "^application/pdf.*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "pdf" );
		} else if( mime.toString().matches( "^.*word$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "word" );
		} else if( mime.toString().matches( "^.*excel$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "excel" );
		} else if( mime.toString().matches( "^.*powerpoint$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "powerpoint" );
		} else if( mime.toString().matches( "^text/plain.*$" ) ) {
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
		
		Options options = new Options();
		options.addOption("o", "output directory", true, "The directory to contain the output XML files");
		options.addOption("s", "Solr URL", true, "The URL of the required Solr Instance");
		options.addOption("t", "Include text in XML", false, "Include text in XML in output files");
		
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
	   	
		   	parseWarcFiles(outputDir, solrUrl, cli_args, isTextRequired);
		
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
	public static void parseWarcFiles(String outputDir, String solrUrl, String[] args, boolean isTextRequired) throws NoSuchAlgorithmException, MalformedURLException, IOException, TransformerFactoryConfigurationError, TransformerException, SolrServerException{
		
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
	private static void printUsage(Options options) {
	      HelpFormatter helpFormatter = new HelpFormatter( );
	      helpFormatter.setWidth( 80 );
	      helpFormatter.printHelp( CLI_USAGE, CLI_HEADER, options, CLI_FOOTER );
	    }
}
