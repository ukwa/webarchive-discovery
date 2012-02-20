package uk.bl.wap.hadoop.tika;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import uk.bl.wap.util.solr.SolrFields;
import uk.bl.wap.util.solr.WctEnricher;
import uk.bl.wap.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private static final String SOLR_DEFAULT = "http://explorer-private:8080/solr/";
	private static final String SOLR_IMAGE = "http://explorer-private:6092/solr/";
	private static final String SOLR_MEDIA = "http://explorer-private:6093/solr/";
	private static final int BATCH = 50;

	HashMap<String, SolrServer> servers = new HashMap<String, SolrServer>();
	HashMap<String,Collection<SolrInputDocument>> docs = new HashMap<String,Collection<SolrInputDocument>>();

	public ArchiveTikaReducer() {
		try {
			servers.put( SOLR_DEFAULT, new CommonsHttpSolrServer( SOLR_DEFAULT ) );
			servers.put( SOLR_IMAGE, new CommonsHttpSolrServer( SOLR_IMAGE ) );
			servers.put( SOLR_MEDIA, new CommonsHttpSolrServer( SOLR_MEDIA ) );
			docs.put( SOLR_DEFAULT, new ArrayList<SolrInputDocument>() );
			docs.put( SOLR_IMAGE, new ArrayList<SolrInputDocument>() );
			docs.put( SOLR_MEDIA, new ArrayList<SolrInputDocument>() );
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}

	@Override
	public void reduce( Text key, Iterator<WritableSolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		WritableSolrRecord solr;
		WctEnricher wct = new WctEnricher( key.toString() );

		while( values.hasNext() ) {
			solr = values.next();
			wct.addWctMetadata( solr );
			processDoc( solr.doc );
			output.collect( new Text( "" ), new Text( ( String ) solr.doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE ) ) );
		}
		processServers( true );
	}

	private void processDoc( SolrInputDocument doc ) {
		String mime = ( String ) doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE );
		if( mime.matches( "^(?:image).*$" ) ) {
			docs.get( SOLR_IMAGE ).add( doc );
		} else if( mime.matches( "^(?:(audio|video)).*$" ) ) {
			docs.get( SOLR_MEDIA ).add( doc );
		} else {
			docs.get( SOLR_DEFAULT ).add( doc );
		}
		processServers( false );
	}

	private void processServers( Boolean flush ) {
		SolrServer server;
		Collection<SolrInputDocument> collection;
		Iterator<String> iterator = docs.keySet().iterator();
		while( iterator.hasNext() ) {
			String key = iterator.next();
			collection = docs.get( key );
			if( ( flush && collection.size() != 0 ) || collection.size() >= BATCH ) {
				server = servers.get( key );
				try {
					server.add( docs.get( key ) );
					docs.get( key ).clear();
				} catch( Exception e ) {
					e.printStackTrace();
				}
			}
		}
	}
} 
