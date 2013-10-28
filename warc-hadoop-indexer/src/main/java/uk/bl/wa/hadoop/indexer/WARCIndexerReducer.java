package uk.bl.wa.hadoop.indexer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;

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
import org.apache.solr.common.SolrException;

import uk.bl.wa.util.solr.WctEnricher;
import uk.bl.wa.util.solr.WctFields;
import uk.bl.wa.util.solr.WritableSolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private static Log log = LogFactory.getLog( WARCIndexerReducer.class );

	private CloudSolrServer solrServer;
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
		try {
			if( !dummyRun ) {
				solrServer = new CloudSolrServer( conf.getString( "warc.solr.zookeeper" ) );
				solrServer.setDefaultCollection( collection );
			}
		} catch( MalformedURLException e ) {
			e.printStackTrace();
		}
	}

	@Override
	public void reduce( Text key, Iterator<WritableSolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		WritableSolrRecord solr;
		WctEnricher wct;

		while( values.hasNext() ) {
			solr = values.next();
			if( solr.doc.containsKey( WctFields.WCT_INSTANCE_ID ) ) {
				wct = new WctEnricher( key.toString() );
				wct.addWctMetadata( solr );
			}
			try {
				if( !dummyRun ) {
					this.solrServer.add( solr.doc );
				} else {
					log.info( "DUMMY_RUN: Skipping addition of doc: " + solr.doc.getField( "id" ).getFirstValue() );
				}
			} catch( SolrServerException e ) {
				e.printStackTrace();
			} catch( SolrException e ) {
				// To catch the protected RemoteSolrException (which extends SolrException).
				e.printStackTrace();
			}
		}
	}
}
