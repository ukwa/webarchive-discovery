package uk.bl.wa.hadoop.indexer;

import static uk.bl.wa.hadoop.indexer.WritableSolrRecord.ARC_TYPE;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.warc.WARCConstants;

import uk.bl.wa.util.solr.SolrFields;
import uk.bl.wa.util.solr.SolrRecord;
import uk.bl.wa.util.solr.WctEnricher;
import uk.bl.wa.util.solr.WctFields;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private static Log log = LogFactory.getLog( WARCIndexerReducer.class );

	private SolrServer solrServer;
	private boolean dummyRun;

	public WARCIndexerReducer() {}

	/**
	 * Sets up our SolrServer.
	 * Presumes the existence of either "warc.solr.zookepers" or "warc.solr.servers" in the config.
	 */
	@Override
	public void configure( JobConf job ) {
		log.info( "Configuring..." );
		// Get config from job property:
		Config conf = ConfigFactory.parseString( job.get( WARCIndexerRunner.CONFIG_PROPERTIES ) );

		this.dummyRun = conf.getBoolean( "warc.solr.dummy_run" );
		try {
			if( !dummyRun ) {
				if( conf.hasPath( "warc.solr.zookeepers" ) ) {
					solrServer = new CloudSolrServer( conf.getString( "warc.solr.zookeepers" ) );
				} else {
					solrServer = new LBHttpSolrServer( conf.getString( "warc.solr.servers" ).split( "," ) );
				}
			}
		} catch( MalformedURLException e ) {
			log.error( "WARCIndexerReducer.configure(): " + e.getMessage() );
		}
	}

	@Override
	public void reduce( Text key, Iterator<WritableSolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		WctEnricher wct;
		TreeSet<String> crawlDates = new TreeSet<String>();

		// Iterate over values, checking if this is a response/revisit, building up a list of timestamps.
		WritableSolrRecord wsr;
		SolrRecord solr;
		String type = null;
		SolrInputDocument oDoc = null;
		while( values.hasNext() ) {
			wsr = values.next();
			solr = wsr.getSolrRecord();

			// Add additional metadata for WCT Instances.
			if( solr.doc.containsKey( WctFields.WCT_INSTANCE_ID ) ) {
				wct = new WctEnricher( ( String ) solr.doc.getFieldValue( WctFields.WCT_INSTANCE_ID ) );
				wct.addWctMetadata( solr );
			}

			// If this is a 'response' or an ARC (i.e. null) record...
			type = wsr.getType();
			if( type.equals( ARC_TYPE ) || type.equals( WARCConstants.RESPONSE ) ) {
				// If oDoc already set, compare dates.
				if( oDoc != null ) {
					String currentEarliest = ( String ) oDoc.getFieldValue( SolrFields.WAYBACK_DATE );
					String toCheck = ( String ) solr.doc.getFieldValue( SolrFields.WAYBACK_DATE );
					if( currentEarliest.compareTo( toCheck ) == 1 ) {
						oDoc = solr.doc;
					}
				} else {
					oDoc = solr.doc;
				}
			}
			crawlDates.add( ( String ) solr.doc.getFieldValue( SolrFields.CRAWL_DATE ) );
		}
		// If we haven't found a response (i.e. just a lot of revisits) something's wrong.
		if( oDoc == null ) {
			log.error( "No appropriate response record found for: " + key + " (" + type + ")" );
			return;
		} else {
			// Set the multivalued field with our (sorted) revisited dates.
			oDoc.setField( SolrFields.CRAWL_DATES, crawlDates.toArray( new String[ crawlDates.size() ] ) );
		}

		if( !dummyRun ) {
			try {
				solrServer.add( oDoc );
			} catch( SolrServerException e ) {
				log.error( "SolrServer.add(): " + e.getMessage(), e );
			}
		} else {
			log.info( "DUMMY_RUN: Skipping addition of doc: " + oDoc.getField( "id" ).getFirstValue() );
		}
	}
}
