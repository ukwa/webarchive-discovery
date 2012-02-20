package uk.bl.wap.hadoop.tika;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.ArchiveUtils;

import uk.bl.wap.hadoop.WritableArchiveRecord;
import uk.bl.wap.util.solr.SolrFields;
import uk.bl.wap.util.solr.TikaExtractor;
import uk.bl.wap.util.solr.WctFields;
import uk.bl.wap.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {
	SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
	String workingDirectory = "";
	TikaExtractor tika = new TikaExtractor();
	MessageDigest md5;
	HashMap<String, String> map = new HashMap<String, String>();
	FileSystem hdfs;

	@Override
	public void configure( JobConf job ) {
		try {
			this.hdfs = FileSystem.get( job );
			URI[] uris = DistributedCache.getCacheFiles( job );
			if( uris != null ) {
				for( URI uri : uris ) {
					FSDataInputStream input = hdfs.open( new Path( uri.getPath()) );
					String line;
					String[] values;
					while( ( line = input.readLine() ) != null ) {
						values = line.split( "\t" );
						map.put( values[ 0 ], values[ 1 ] );
					}
				}
			}
		} catch( IOException e ) {
			e.printStackTrace();
		}
	}

	public ArchiveTikaMapper() throws IOException {
		try {
			md5 = MessageDigest.getInstance( "MD5" );
		} catch( NoSuchAlgorithmException e ) {
			System.err.println( "ArchiveTikaMapper(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, WritableSolrRecord> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();
		WritableSolrRecord solr = null;

		if( !header.getHeaderFields().isEmpty() ) {
			solr = tika.extract( value.getPayload() );

			String wctID = this.getWctTi( key.toString() );
			String waybackDate = ( header.getDate().replaceAll( "[^0-9]", "" ) );

			solr.doc.setField( SolrFields.SOLR_ID, waybackDate + "/" + new String( Base64.encodeBase64( md5.digest( header.getUrl().getBytes( "UTF-8" ) ) ) ) );
			solr.doc.setField( SolrFields.SOLR_DIGEST, header.getDigest() );
			solr.doc.setField( SolrFields.SOLR_URL, header.getUrl() );
			try {
				solr.doc.setField( SolrFields.SOLR_TIMESTAMP, formatter.format( ArchiveUtils.parse14DigitDate( waybackDate ) ) );
			} catch( ParseException p ) {
				p.printStackTrace();
			}
			solr.doc.setField( SolrFields.SOLR_REFERRER_URI, map.get( header.getUrl() ) );
			solr.doc.setField( WctFields.WCT_WAYBACK_DATE, waybackDate );
			solr.doc.setField( WctFields.WCT_INSTANCE_ID, wctID );

			this.processMime( solr, header );
			this.stripNull( solr );
			output.collect( new Text( wctID ), solr );
		}
	}

	private String getWctTi( String warcName ) {
		Pattern pattern = Pattern.compile( "^[A-Z]+-\\b([0-9]+)\\b.*\\.w?arc(\\.gz)?$" );
		Matcher matcher = pattern.matcher( warcName );
		if( matcher.matches() ) {
			return matcher.group( 1 );
		}
		return "";
	}

	private void processMime( WritableSolrRecord solr, ArchiveRecordHeader header ) {
		StringBuilder mime = new StringBuilder();
		mime.append( ( ( String ) solr.doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE ) ) );
		if( mime.toString().isEmpty() ) {
			if( header.getHeaderFieldKeys().contains( "WARC-Identified-Payload-Type" ) ) {
				mime.append( ( ( String ) header.getHeaderFields().get( "WARC-Identified-Payload-Type" ) ) );
			} else {
				mime.append( header.getMimetype() );
			}
		}
		solr.doc.setField( SolrFields.SOLR_CONTENT_TYPE, mime.toString().replaceAll( ";.*$", "" ) );

		if( mime.toString().matches( "^(?:image).*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "image" );
		} else if( mime.toString().matches( "^(?:(audio|video)).*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "media" );
		} else if( mime.toString().matches( "^.*htm.*$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "html" );
		} else if( mime.toString().matches( "^.*pdf$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "pdf" );
		} else if( mime.toString().matches( "^.*word$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "word" );
		} else if( mime.toString().matches( "^.*excel$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "excel" );
		} else if( mime.toString().matches( "^.*powerpoint$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "powerpoint" );
		} else if( mime.toString().matches( "^text/plain$" ) ) {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "text" );
		} else {
			solr.doc.setField( SolrFields.SOLR_NORMALISED_CONTENT_TYPE, "other" );
		}		
	}

	private void stripNull( WritableSolrRecord solr ) {
		Iterator<String> keys = solr.doc.keySet().iterator();
		String key;
		while( keys.hasNext() ) {
			key = keys.next();
			if( solr.doc.get( key ) == null ) {
				solr.doc.remove( key );
			}
		}
	}
}