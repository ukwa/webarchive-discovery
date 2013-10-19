package uk.bl.wap.hadoop.tika;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.http.HttpHost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wap.solr.QueueingHttpSolrServer;
import uk.bl.wap.util.solr.WctEnricher;
import uk.bl.wap.util.solr.WctFields;
import uk.bl.wap.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private QueueingHttpSolrServer solrServer;
	private int batchSize;

	public ArchiveTikaReducer() {}

	@Override
	public void configure( JobConf job ) {
		// Get config from job property:
		Config conf = ConfigFactory.parseString(job.get(ArchiveTikaExtractor.CONFIG_PROPERTIES));
		
		this.batchSize = conf.getInt( "warc.solr.batch_size" );
		try {
			if( conf.getString( "http.proxy.host" ) != null && conf.hasPath( "http.proxy.port" ) ) {
				DefaultHttpClient httpclient = new DefaultHttpClient();
				HttpHost proxy = new HttpHost( conf.getString( "http.proxy.host" ), conf.getInt( "http.proxy.port" ), "http" );
				httpclient.getParams().setParameter( ConnRoutePNames.DEFAULT_PROXY, proxy );
				System.out.println( "Using proxy: " + proxy.toURI() );
				this.solrServer = new QueueingHttpSolrServer( conf.getString( "warc.solr.server" ), this.batchSize, httpclient );
			} else {
				this.solrServer = new QueueingHttpSolrServer( conf.getString( "warc.solr.server" ), this.batchSize );
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
				this.solrServer.add( solr.doc );
			} catch( SolrServerException e ) {
				e.printStackTrace();
			} catch( SolrException e ) {
				// To catch the protected RemoteSolrException (which extends SolrException).
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		try {
			this.solrServer.flush();
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}
}
