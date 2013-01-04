/**
 * 
 */
package uk.bl.wap.indexer;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.RESPONSE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.tika.io.TikaInputStream;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.util.ArchiveUtils;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import uk.bl.wap.util.WARCRecordUtils;
import uk.bl.wap.util.solr.SolrFields;
import uk.bl.wap.util.solr.TikaExtractor;
import uk.bl.wap.util.solr.WctFields;
import uk.bl.wap.util.solr.WritableSolrRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexer {
	
	TikaExtractor tika = new TikaExtractor();
	MessageDigest md5 = null;
	AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();
	SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );

	public WARCIndexer() throws NoSuchAlgorithmException {
		md5 = MessageDigest.getInstance( "MD5" );
	}

	public WritableSolrRecord extract( String archiveName, ArchiveRecord record ) throws IOException {
		ArchiveRecordHeader header = record.getHeader();
		WritableSolrRecord solr = new WritableSolrRecord();

		if( !header.getHeaderFields().isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) && !header.getHeaderValue( HEADER_KEY_TYPE ).equals( RESPONSE ) ) {
				return null;
			}
			
			if( header.getUrl() == null ) return null;
			
			for( String h : header.getHeaderFields().keySet()) {
				System.out.println("ArchiveHeader: "+h+" -> "+header.getHeaderValue(h));
			}
			
			if( ! record.hasContentHeaders() ) return null;
			
			// Basic headers
			
			// Date
			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );
			solr.doc.setField( SolrFields.WAYBACK_DATE, waybackDate );
			
			// 
			solr.doc.setField( SolrFields.SOLR_ID, waybackDate + "/" + new String( Base64.encodeBase64( md5.digest( header.getUrl().getBytes( "UTF-8" ) ) ) ) );
			solr.doc.setField( SolrFields.SOLR_DIGEST, header.getDigest() );
			solr.doc.setField( SolrFields.SOLR_URL, header.getUrl() );
			solr.doc.setField( SolrFields.SOLR_DOMAIN, canon.urlStringToKey( header.getUrl() ).split( "/" )[ 0 ] );

			try {
				solr.doc.setField( SolrFields.SOLR_TIMESTAMP, formatter.format( ArchiveUtils.parse14DigitDate( waybackDate ) ) );
			} catch( ParseException p ) {
				p.printStackTrace();
			}
			
			// Parse body:

			String referrer = null;
			InputStream tikainput = null;
			String statusCode = null;
			if( record instanceof WARCRecord ) {
				String firstLine[] = HttpParser.readLine(record, "UTF-8").split(" ");
				statusCode = firstLine[1];
				Header[] headers = HttpParser.parseHeaders(record, "UTF-8");
				for( Header h : headers ) {
					System.out.println("HttpHeader: "+h.getName()+" -> "+h.getValue());
					if( h.getName().equals("Referer"))
						referrer = h.getValue();
				}
				// No need for this, as the headers have already been read from the InputStream:
				// WARCRecordUtils.getPayload(record);
				tikainput = record;
			
			} else if ( record instanceof ARCRecord ) {
				ARCRecord arcr = (ARCRecord) record;
				statusCode = ""+arcr.getStatusCode();
				for( Header h : arcr.getHttpHeaders() ) {
					System.out.println("HttpHeader: "+h.getName()+" -> "+h.getValue());
					if( h.getName().equals("Referer"))
						referrer = h.getValue();
				}
				arcr.skipHttpHeader();
				tikainput = arcr;
				
			} else {
				System.err.println("FAIL!");
				return solr;
			}
			
			// Fields from Http headers:
			if( referrer != null )
				solr.doc.setField( SolrFields.SOLR_REFERRER_URI, referrer );
			System.out.println("Status Code: "+statusCode);
			
			// Parse payload using Tika:
			
			// Mark the start of the payload, and then run Tika on it:
			tikainput = new BufferedInputStream( tikainput );
			tikainput.mark((int) header.getLength());
			solr = tika.extract( solr, tikainput, header.getUrl() );
			
			// Pass on to other extractors as required, resetting the stream before each:
			//tikainput.reset();
			// Entropy, compressibility, fussy hashes, etc.
			// JSoup link extractor for (x)html
			
			// These extractors don't need to re-read the payload:
			// Postcode Extractor (based on text extracted by Tika)
			// Named entity detection
			// WctEnricher, currently invoked in the reduce stage to lower query hits.
			
		}
		return solr;
	}
	
	/**
	 * 
	 * @param args
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws TransformerException 
	 * @throws TransformerFactoryConfigurationError 
	 */
	public static void main( String[] args ) throws NoSuchAlgorithmException, IOException, TransformerFactoryConfigurationError, TransformerException {
		WARCIndexer windex = new WARCIndexer();
		for( String f : args ) {
			ArchiveReader reader = ArchiveReaderFactory.get(f);
			Iterator<ArchiveRecord> ir = reader.iterator();
			while( ir.hasNext() ) {
				ArchiveRecord rec = ir.next();
				WritableSolrRecord doc = windex.extract("",rec);
				if( doc != null ) {
					prettyPrintXML(ClientUtils.toXML(doc.doc));
					//break;
				}
				System.out.println(" ---- ---- ");
			}
		}
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
}
