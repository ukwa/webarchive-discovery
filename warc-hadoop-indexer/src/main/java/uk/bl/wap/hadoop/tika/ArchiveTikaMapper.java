package uk.bl.wap.hadoop.tika;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wap.hadoop.WritableArchiveRecord;
import uk.bl.wap.indexer.WARCIndexer;
import uk.bl.wap.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {
	private WARCIndexer windex;

	@Override
	public void configure( JobConf conf ) {
		try {
			this.windex = new WARCIndexer( conf );
		} catch( NoSuchAlgorithmException e ) {
			System.err.println( "ArchiveTikaMapper.configure(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, WritableSolrRecord> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();
		WritableSolrRecord solr = new WritableSolrRecord();

		if( !header.getHeaderFields().isEmpty() ) {
			solr = windex.extract( key.toString(), value.getRecord() );

			String oKey = null;
			try {
				URI uri = new URI( header.getUrl() );
				oKey = uri.getHost();
				if( oKey != null )
					output.collect( new Text( oKey ), solr );
			} catch( Exception e ) {
				System.err.println( e.getMessage() + "; " + header.getUrl() + "; " + oKey + "; " + solr );
			}
		}
	}
}