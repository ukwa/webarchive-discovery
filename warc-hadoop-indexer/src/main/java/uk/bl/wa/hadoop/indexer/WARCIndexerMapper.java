package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.hadoop.Solate;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements
		Mapper<Text, WritableArchiveRecord, IntWritable, WritableSolrRecord> {
	private static final Log LOG = LogFactory.getLog( WARCIndexerMapper.class );

	private WARCIndexer windex;

	private Solate sp;

	public WARCIndexerMapper() {
		try {
			Properties props = new Properties();
			props.load(getClass().getResourceAsStream("/log4j-override.properties"));
			PropertyConfigurator.configure(props);
		} catch (IOException e1) {
			LOG.error("Failed to load log4j config from properties file.");
		}
	}

	@Override
	public void configure( JobConf job ) {
		try {
			// Get config from job property:
			Config config = ConfigFactory.parseString( job.get( WARCIndexerRunner.CONFIG_PROPERTIES ) );
			// Initialise indexer:
			this.windex = new WARCIndexer( config );
			// Re-configure logging:
			String zkHost = config.getString(SolrWebServer.CONF_ZOOKEEPERS);
			String collection = config.getString(SolrWebServer.COLLECTION);
			int numShards = config.getInt(SolrWebServer.NUM_SHARDS);
			sp = new Solate(zkHost, collection, numShards);

		} catch( NoSuchAlgorithmException e ) {
			LOG.error("WARCIndexerMapper.configure(): " + e.getMessage());
		}
	}

	@Override
	public void map(Text key, WritableArchiveRecord value,
			OutputCollector<IntWritable, WritableSolrRecord> output,
			Reporter reporter) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();

		if( !header.getHeaderFields().isEmpty() ) {
			SolrRecord solr = windex.extract( key.toString(), value.getRecord() );

			if( solr == null ) {
				LOG.debug( "WARCIndexer returned NULL for: " + header.getUrl() );
				return;
			}

			String host = ( String ) solr.getFieldValue( SolrFields.SOLR_HOST );
			if( host == null ) {
				host = "unknown.host";
			}
			
			IntWritable oKey = new IntWritable(sp.getPartition(null,
					solr.getSolrDocument()));
			try {
				WritableSolrRecord wsolr = new WritableSolrRecord( solr );
				output.collect( oKey, wsolr );
			} catch( Exception e ) {
				LOG.error( e.getClass().getName() + ": " + e.getMessage() + "; " + header.getUrl() + "; " + oKey + "; " + solr );
			}
		}
	}


}
