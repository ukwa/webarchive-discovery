package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.solr.WctEnricher;
import uk.bl.wa.solr.WctFields;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private static Log log = LogFactory.getLog( WARCIndexerReducer.class );

	private SolrWebServer solrServer;
	private int batchSize;
	private boolean dummyRun;
	private ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

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
		
		solrServer = new SolrWebServer(conf);
		log.info( "Initialisation complete." );
	}

	@Override
	public void reduce( Text key, Iterator<WritableSolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		WctEnricher wct;
		WritableSolrRecord wsr;
		SolrRecord solr;

		while( values.hasNext() ) {
			wsr = values.next();
			solr = wsr.getSolrRecord();

			// Add additional metadata for WCT Instances.
			if( solr.containsKey( WctFields.WCT_INSTANCE_ID ) ) {
				wct = new WctEnricher( key.toString() );
				wct.addWctMetadata( solr );
			}
			if( !dummyRun ) {
				docs.add( solr.getSolrDocument() );
				// Have we exceeded the batchSize?
				checkSubmission( docs, batchSize );
			} else {
				log.info( "DUMMY_RUN: Skipping addition of doc: " + solr.getField( "id" ).getFirstValue() );
			}
		}
	}

	/**
	 * If we have at least one document unsubmitted, make sure we submit it.
	 */
	@Override
	public void close() {
		checkSubmission( docs, 1 );
	}

	/**
	 * Checks whether a List of docs has exceeded a given limit
	 * and if so, submits them.
	 * 
	 * @param docs
	 * @param limit
	 */
	private void checkSubmission( List<SolrInputDocument> docs, int limit ) {
		UpdateResponse response;
		if( docs.size() > 0 && docs.size() >= limit ) {
			try {
				response = solrServer.add( docs );
				log.info( "Submitted " + docs.size() + " docs [" + response.getStatus() + "]" );
				docs.clear();
			} catch( Exception e ) {
				// SOLR-5719 possibly hitting us here;
				// CloudSolrServer.RouteException
				log.error( "WARCIndexerReducer.reduce() - sleeping for 5 minutes: " + e.getMessage(), e );
				try {
				    Thread.sleep(1000*60*5);
				} catch(InterruptedException ex) {
					log.warn("Sleep between Solr submissions was interrupted!");
				}
			}
		}
	}
}
