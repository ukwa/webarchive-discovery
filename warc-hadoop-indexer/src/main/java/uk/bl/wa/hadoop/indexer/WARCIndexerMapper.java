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
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.apache.solr.hadoop.Solate;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements
		Mapper<Text, WritableArchiveRecord, IntWritable, WritableSolrRecord> {
	private static final Log LOG = LogFactory.getLog( WARCIndexerMapper.class );

	static enum MyCounters {
		NUM_RECORDS, NUM_ERRORS, NUM_NULLS, NUM_EMPTY_HEADERS
	}

	private String mapTaskId;
	private String inputFile;
	private int noRecords = 0;

	private WARCIndexer windex;

	private Solate sp = null;
	private int numShards = 1;

	public WARCIndexerMapper() {
		try {
			// Re-configure logging:
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
			// Set up sharding:
			numShards = config.getInt(SolrWebServer.NUM_SHARDS);
			if (config.hasPath(SolrWebServer.CONF_ZOOKEEPERS)) {
				String zkHost = config.getString(SolrWebServer.CONF_ZOOKEEPERS);
				String collection = config.getString(SolrWebServer.COLLECTION);
				sp = new Solate(zkHost, collection, numShards);
			}
			// Other properties:
			mapTaskId = job.get("mapred.task.id");
			inputFile = job.get("map.input.file");

		} catch( NoSuchAlgorithmException e ) {
			LOG.error("WARCIndexerMapper.configure(): " + e.getMessage());
		}
	}

	@Override
	public void map(Text key, WritableArchiveRecord value,
			OutputCollector<IntWritable, WritableSolrRecord> output,
			Reporter reporter) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();

		noRecords++;

		try {
			if (!header.getHeaderFields().isEmpty()) {
				// Do the indexing:
				SolrRecord solr = windex.extract(key.toString(),
						value.getRecord());

				// If there is no result, report it
				if (solr == null) {
					LOG.debug("WARCIndexer returned NULL for: "
							+ header.getUrl());
					reporter.incrCounter(MyCounters.NUM_NULLS, 1);
					return;
				}

				// String host = (String)
				// solr.getFieldValue(SolrFields.SOLR_HOST);
				// if (host == null) {
				// host = "unknown.host";
				// }

				// Get the right key for the right partition:
				IntWritable oKey = null;
				if (sp != null) {
					oKey = new IntWritable(sp.getPartition(null,
							solr.getSolrDocument()));
				} else {
					// Otherwise use a random assignment:
					int iKey = (int) (Math.round(Math.random() * numShards));
					oKey = new IntWritable(iKey);
				}

				// Wrap up and collect the result:
				WritableSolrRecord wsolr = new WritableSolrRecord( solr );
				output.collect( oKey, wsolr );

				// Increment record counter:
				reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

				// Occasionally update application-level status
				if ((noRecords % 1000) == 0) {
					reporter.setStatus(noRecords + " processed from "
							+ inputFile);
					// Also assure framework that we are making progress:
					reporter.progress();
				}


			} else {
				// Report headerless records:
				reporter.incrCounter(MyCounters.NUM_EMPTY_HEADERS, 1);

			}

		} catch (Exception e) {
			LOG.error(e.getClass().getName() + ": " + e.getMessage() + "; "
					+ header.getUrl() + "; " + header.getOffset());
			// Increment error counter
			reporter.incrCounter(MyCounters.NUM_ERRORS, 1);

		} catch (OutOfMemoryError e) {
			// Allow processing to continue if a record causes OOME:
			LOG.error("OOME" + e.getClass().getName() + ": " + e.getMessage()
					+ "; " + header.getUrl() + "; " + header.getOffset());
			// Increment error counter
			reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
		}
	}


}
