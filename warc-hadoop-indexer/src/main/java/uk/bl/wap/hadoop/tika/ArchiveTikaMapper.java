package uk.bl.wap.hadoop.tika;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import uk.bl.wap.hadoop.WritableArchiveRecord;
import uk.bl.wap.indexer.WARCIndexer;
import uk.bl.wap.util.solr.WctFields;
import uk.bl.wap.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {
	private WARCIndexer windex;
	private HashMap<String, String> map = new HashMap<String, String>();
	private FileSystem hdfs;
	
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
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}

	public ArchiveTikaMapper() throws IOException {
		try {
			this.windex = new WARCIndexer();
		} catch( NoSuchAlgorithmException e ) {
			System.err.println( "ArchiveTikaMapper(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, WritableSolrRecord> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();
		WritableSolrRecord solr = new WritableSolrRecord();

		if( !header.getHeaderFields().isEmpty() ) {
			solr = windex.extract( key.toString(), value.getRecord() );

			// Also pick up WCT TI ID
			String wctID = this.getWctTi( key.toString() );
			solr.addField( WctFields.WCT_INSTANCE_ID, wctID );
			
			// FIXME: This should either hook on just the host, or be optional, and use a different key for non-WCT collections.

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

}