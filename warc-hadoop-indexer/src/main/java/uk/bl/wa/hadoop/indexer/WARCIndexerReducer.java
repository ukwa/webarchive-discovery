package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

import uk.bl.wa.util.solr.SolrRecord;
import uk.bl.wa.util.solr.WctEnricher;
import uk.bl.wa.util.solr.WctFields;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private static Log log = LogFactory.getLog( WARCIndexerReducer.class );

	private CloudSolrServer solrServer;
	private int batchSize;
	private String collection;
	private boolean dummyRun;

	public WARCIndexerReducer() {}

	@Override
	public void configure( JobConf job ) {
		log.info( "Configuring..." );
		// Get config from job property:
		Config conf = ConfigFactory.parseString( job.get( WARCIndexerRunner.CONFIG_PROPERTIES ) );

		this.collection = conf.getString( "warc.solr.collection" );
		this.dummyRun = conf.getBoolean( "warc.solr.dummy_run" );
		this.batchSize = conf.getInt( "warc.solr.batch_size" );
		try {
			if( !dummyRun ) {
				solrServer = new CloudSolrServer( conf.getString( "warc.solr.zookeeper" ) );
				solrServer.setDefaultCollection( collection );
			}
		} catch( MalformedURLException e ) {
			log.error( "WARCIndexerReducer.configure(): " + e.getMessage() );
		}
	}

	@Override
	public void reduce( Text key, Iterator<WritableSolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		WctEnricher wct;

		ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		while( values.hasNext() ) {
			WritableSolrRecord wsr = values.next();
			SolrRecord solr = wsr.getSolrRecord();
			if( solr.doc.containsKey( WctFields.WCT_INSTANCE_ID ) ) {
				wct = new WctEnricher( key.toString() );
				wct.addWctMetadata( solr );
			}
			if( !dummyRun ) {
				docs.add( solr.doc );
				// Have we exceeded the batchSize?
				checkSubmission( docs, batchSize );
			} else {
				log.info( "DUMMY_RUN: Skipping addition of doc: " + solr.doc.getField( "id" ).getFirstValue() );
			}
		}
		if( !dummyRun ) {
			// Have we any unsubmitted SolrInputDocuments?
			checkSubmission( docs, 0 );
		}
	}

	/**
	 * Checks whether a List of docs has exceeded a given limit
	 * and if so, submits them.
	 * 
	 * @param docs
	 * @param limit
	 */
	private void checkSubmission( List<SolrInputDocument> docs, int limit ) {
		if( docs.size() > 0 && docs.size() >= limit ) {
			try {
				solrServer.add( docs );
			} catch( SolrServerException e ) {
				log.error( "WARCIndexerReducer.reduce(): " + e.getMessage() );
			} catch( IOException i ) {
				log.error( "WARCIndexerReducer.reduce(): " + i.getMessage() );
			}
			docs.clear();
		}
	}
}
