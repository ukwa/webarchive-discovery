/**
 * 
 */
package uk.bl.wap.indexer;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
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

	public WritableSolrRecord extract( ArchiveRecord record ) throws IOException {
		ArchiveRecordHeader header = record.getHeader();
		WritableSolrRecord solr = null;

		if( !header.getHeaderFields().isEmpty() ) {
			Header[] headers = HttpParser.parseHeaders(record, "UTF-8");
			String referrer = null;
			for( Header h : headers ) {
				if( h.getName().equals("Referer"))
					referrer = h.getValue();
			}
			// Parse payload using Tika:
			solr = tika.extract( WARCRecordUtils.getPayload(record) );

			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );

			solr.doc.setField( SolrFields.SOLR_ID, waybackDate + "/" + new String( Base64.encodeBase64( md5.digest( header.getUrl().getBytes( "UTF-8" ) ) ) ) );
			solr.doc.setField( SolrFields.SOLR_DIGEST, header.getDigest() );
			solr.doc.setField( SolrFields.SOLR_URL, header.getUrl() );
			solr.doc.setField( SolrFields.SOLR_DOMAIN, canon.urlStringToKey( header.getUrl() ).split( "/" )[ 0 ] );
			try {
				solr.doc.setField( SolrFields.SOLR_TIMESTAMP, formatter.format( ArchiveUtils.parse14DigitDate( waybackDate ) ) );
			} catch( ParseException p ) {
				p.printStackTrace();
			}
			if( referrer != null )
				solr.doc.setField( SolrFields.SOLR_REFERRER_URI, referrer );
			solr.doc.setField( WctFields.WCT_WAYBACK_DATE, waybackDate );
		}
		return solr;
	}
}
