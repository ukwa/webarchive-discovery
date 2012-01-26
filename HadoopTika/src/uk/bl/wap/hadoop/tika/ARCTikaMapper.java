package uk.bl.wap.hadoop.tika;

/*
 * For JobConf.get() property see:
 * http://hadoop.apache.org/common/docs/r0.18.3/mapred_tutorial.html
 */

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
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

import uk.bl.wap.hadoop.WritableARCRecord;
import uk.bl.wap.util.solr.SolrRecord;
import uk.bl.wap.util.solr.TikaExtractor;

@SuppressWarnings( { "deprecation" } )
public class ARCTikaMapper extends MapReduceBase implements Mapper<Text, WritableARCRecord, Text, SolrRecord> {
	String workingDirectory = "";
	TikaExtractor tika = new TikaExtractor();
	MessageDigest md5;
	HashMap<String, String> map = new HashMap<String, String>();
	FileSystem hdfs;

	@Override
	public void configure( JobConf job ) {
		//this.workingDirectory = job.get( "mapred.work.output.dir" );
		try {
			this.hdfs = FileSystem.get( job );
			URI[] uris = DistributedCache.getCacheFiles( job );
			for( URI uri : uris ) {
				FSDataInputStream input = hdfs.open( new Path( uri.getPath()) );
				String line;
				String[] values;
				while( ( line = input.readLine() ) != null ) {
					values = line.split( "\t" );
					map.put( values[ 0 ], values[ 1 ] );
				}
			}
		} catch( IOException e ) {
			e.printStackTrace();
		}
	}

	public ARCTikaMapper() {
		try {
			md5 = MessageDigest.getInstance( "MD5" );
		} catch( NoSuchAlgorithmException e ) {
			System.err.println( "WARCTikaMapper(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableARCRecord value, OutputCollector<Text, SolrRecord> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();
		SolrRecord sr = null;

		if( !header.getHeaderFields().isEmpty() ) {
			sr = tika.extract( value.getPayload() );

			String wctID = this.getWctTi( key.toString() );
			String waybackDate = ( String ) header.getDate().replaceAll( "[^0-9]", "" );

			String id = waybackDate + "/" + new String( Base64.encodeBase64( md5.digest( header.getUrl().getBytes( "UTF-8" ) ) ) );
			sr.setId( id );

			sr.setHash( ( String ) header.getDigest() );
			sr.setWctUrl( ( String ) header.getUrl() );
			sr.setTimestamp( ( String ) header.getDate() );
			sr.setWctWaybackDate( waybackDate );
			sr.setWctInstanceId( wctID );

			try {
				if( sr.getContentType().equals( "" ) ) {
					sr.setContentType( ( String ) header.getHeaderFields().get( "WARC-Identified-Payload-Type" ) );
				}
				output.collect( new Text( wctID ), sr );
			} catch( Exception e ) {
				System.err.println( e.getMessage() );
			}
		}
	}

	private String getWctTi( String ARCName ) {
		Pattern pattern = Pattern.compile( "^BL-\\b([0-9]+)\\b.*\\.ARC(\\.gz)?$" );
		Matcher matcher = pattern.matcher( ARCName );
		if( matcher.matches() ) {
			return matcher.group( 1 );
		}
		return "";
	}
}