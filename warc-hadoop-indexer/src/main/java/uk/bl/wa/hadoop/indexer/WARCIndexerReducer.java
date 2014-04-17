package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import uk.bl.wa.apache.solr.hadoop.Solate;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.solr.WctEnricher;
import uk.bl.wa.solr.WctFields;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerReducer extends MapReduceBase implements
		Reducer<IntWritable, WritableSolrRecord, Text, Text> {
	private static Log log = LogFactory.getLog( WARCIndexerReducer.class );

	private SolrServer solrServer;
	private int batchSize;
	private boolean dummyRun;
	private ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	private int numberOfSequentialFails = 0;

	private FileSystem fs;
	private Path solrHomeDir = null;
	private Path outputDir;
	private String shardPrefix = "shard";

	static enum MyCounters {
		NUM_RECORDS, NUM_ERRORS, NUM_DROPPED_RECORDS
	}

	public WARCIndexerReducer() {
		try {
			Properties props = new Properties();
			props.load(getClass().getResourceAsStream("/log4j-override.properties"));
			PropertyConfigurator.configure(props);
		} catch (IOException e1) {
			log.error("Failed to load log4j config from properties file.");
		}
	}

	/**
	 * Sets up our SolrServer.
	 * Presumes the existence of either "warc.solr.zookepers" or "warc.solr.servers" in the config.
	 */
	@Override
	public void configure( JobConf job ) {
		log.info( "Configuring reducer, including Solr connection..." );
		
		// Get config from job property:
		Config conf = ConfigFactory.parseString( job.get( WARCIndexerRunner.CONFIG_PROPERTIES ) );

		this.dummyRun = conf.getBoolean( "warc.solr.dummy_run" );
		this.batchSize = conf.getInt( "warc.solr.batch_size" );
		
		try {
			// Filesystem:
			job.setBoolean("fs.hdfs.impl.disable.cache", true);
			fs = FileSystem.get(job);
			// Input:
			solrHomeDir = Solate.findSolrConfig(job,
					WARCIndexerRunner.solrHomeZipName);
			log.info("Found solrHomeDir " + solrHomeDir);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("FAILED in reducer configuration: " + e);
		}
		// Output:
		outputDir = new Path(conf.getString(SolrWebServer.HDFS_OUTPUT_PATH));

		// solrServer = new SolrWebServer(conf);
		log.info( "Initialisation complete." );
	}

	@Override
	public void reduce(IntWritable key, Iterator<WritableSolrRecord> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		WctEnricher wct;
		WritableSolrRecord wsr;
		SolrRecord solr;

		// Get the slice number, but counting from 1 instead of 0:
		int slice = key.get() + 1;

		// Defined the output directory accordingly:
		Path outputShardDir = new Path(fs.getHomeDirectory() + "/" + outputDir,
				this.shardPrefix + slice);

		// Fire up a server:
		solrServer = Solate.createEmbeddedSolrServer(solrHomeDir, fs,
				outputDir, outputShardDir);

		// Go through the documents for this shard:
		long noValues = 0;
		while( values.hasNext() ) {
			wsr = values.next();
			solr = wsr.getSolrRecord();
			noValues++;

			// Add additional metadata for WCT Instances.
			if( solr.containsKey( WctFields.WCT_INSTANCE_ID ) ) {
				wct = new WctEnricher( key.toString() );
				wct.addWctMetadata( solr );
			}
			if( !dummyRun ) {
				docs.add( solr.getSolrDocument() );
				// Have we exceeded the batchSize?
				checkSubmission(docs, batchSize, reporter);
			} else {
				log.info( "DUMMY_RUN: Skipping addition of doc: " + solr.getField( "id" ).getFirstValue() );
			}

			// Occasionally update application-level status:
			if ((noValues % 1000) == 0) {
				reporter.setStatus("For shard " + this.shardPrefix
						+ ", processed " + noValues + " documents.");
			}
		}

		try {
			/**
			 * If we have at least one document unsubmitted, make sure we submit
			 * it.
			 */
			checkSubmission(docs, 1, reporter);
			// Commit, and block until the changes have been flushed.
			solrServer.commit(true, false);
			// And shut it down.
			solrServer.shutdown();
		} catch (Exception e) {
			log.error("ERROR on commit: " + e);
			e.printStackTrace();
		}

	}

	@Override
	public void close() {
	}

	/**
	 * Checks whether a List of docs has exceeded a given limit and if so,
	 * submits them.
	 * 
	 * @param docs
	 * @param limit
	 * @param reporter
	 */
	private void checkSubmission(List<SolrInputDocument> docs, int limit,
			Reporter reporter) {
		if( docs.size() > 0 && docs.size() >= limit ) {
			try {
				// Inform that there is progress (still-alive):
				reporter.progress();
				// Add the documents:
				UpdateResponse response = solrServer.add(docs);
				log.info( "Submitted " + docs.size() + " docs [" + response.getStatus() + "]" );
				// Update document counter:
				reporter.incrCounter(MyCounters.NUM_RECORDS, docs.size());
				// All good:
				docs.clear();
				numberOfSequentialFails = 0;
			} catch( Exception e ) {
				// Count up repeated fails:
				numberOfSequentialFails++;

				// If there have been a lot of fails, drop the records
				// (we have seen some "Invalid UTF-8 character 0xfffe at char"
				// so this avoids bad data blocking job completion)
				if (this.numberOfSequentialFails >= 3) {
					log.error("Submission has repeatedly failed - assuming bad data and dropping these "
							+ docs.size() + " records.");
					reporter.incrCounter(MyCounters.NUM_DROPPED_RECORDS,
							docs.size());
					docs.clear();
				}

				// SOLR-5719 possibly hitting us here;
				// CloudSolrServer.RouteException
				log.error("Sleeping for 1 minute: "
								+ e.getMessage(), e);
				// Also add a report for this condition:
				reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
				try {
					Thread.sleep(1000 * 60 * 1);
				} catch(InterruptedException ex) {
					log.warn("Sleep between Solr submissions was interrupted!");
				}
			}
		}
	}
}
