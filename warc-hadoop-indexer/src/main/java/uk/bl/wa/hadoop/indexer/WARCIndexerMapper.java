package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {

	private static final Log LOG = LogFactory.getLog(WARCIndexerMapper.class);

	private WARCIndexer windex;

	@Override
	public void configure( JobConf job ) {
		try {
			// Get config from job property:
			Config config = ConfigFactory.parseString(job.get(WARCIndexerRunner.CONFIG_PROPERTIES));
			// Initialise indexer:
			this.windex = new WARCIndexer( config );
		} catch( NoSuchAlgorithmException e ) {
			LOG.error( "ArchiveTikaMapper.configure(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, WritableSolrRecord> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();
		WritableSolrRecord solr = new WritableSolrRecord();

		if( !header.getHeaderFields().isEmpty() ) {
			solr = windex.extract( key.toString(), value.getRecord() );
			
			if( solr == null ) {
				LOG.debug("WARCIndexer returned NULL for: "+ header.getUrl());
			}

			String oKey = null;
			try {
				URI uri = new URI( header.getUrl() );
				oKey = uri.getHost();
				if( oKey != null )
					output.collect( new Text( oKey ), solr );
			} catch( Exception e ) {
				LOG.error(e.getClass().getName() + ": " + e.getMessage() + "; " + header.getUrl() + "; " + oKey + "; " + solr );
			}
		}
	}
}