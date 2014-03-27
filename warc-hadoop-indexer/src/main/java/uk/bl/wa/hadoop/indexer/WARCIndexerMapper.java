package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, WritableSolrRecord> {
	private static final Log LOG = LogFactory.getLog( WARCIndexerMapper.class );

	private WARCIndexer windex;

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
		} catch( NoSuchAlgorithmException e ) {
			LOG.error( "ArchiveTikaMapper.configure(): " + e.getMessage() );
		}
	}

	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, WritableSolrRecord> output, Reporter reporter ) throws IOException {
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
			
			Text oKey = new Text( host );
			try {
				WritableSolrRecord wsolr = new WritableSolrRecord( solr );
				output.collect( oKey, wsolr );
			} catch( Exception e ) {
				LOG.error( e.getClass().getName() + ": " + e.getMessage() + "; " + header.getUrl() + "; " + oKey + "; " + solr );
			}
		}
	}


}
