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

import uk.bl.wap.solr.QueueingHttpSolrServer;
import uk.bl.wap.util.solr.SolrFields;
import uk.bl.wap.util.solr.WctEnricher;
import uk.bl.wap.util.solr.WritableSolrRecord;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaReducer extends MapReduceBase implements Reducer<Text, WritableSolrRecord, Text, Text> {
	private QueueingHttpSolrServer solrDefault;
	private QueueingHttpSolrServer solrImage;
	private QueueingHttpSolrServer solrMedia;
	private int batchSize;

	public ArchiveTikaReducer() {}

	@Override
	public void configure( JobConf job ) {
		this.batchSize = job.getInt( "solr.batch.size", 50 );
		try {
			this.solrDefault = new QueueingHttpSolrServer( job.get( "solr.default", "http://explorer-private:8080/solr/" ), this.batchSize );
			this.solrImage = new QueueingHttpSolrServer( job.get( "solr.image", "http://explorer-private:6092/solr/" ), this.batchSize );
			this.solrMedia = new QueueingHttpSolrServer( job.get( "solr.media", "http://explorer-private:6093/solr/" ), this.batchSize );
		} catch( MalformedURLException e ) {
			e.printStackTrace();
		}
	}

	@Override
	public void reduce( Text key, Iterator<WritableSolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		WritableSolrRecord solr;
		String mime;
		WctEnricher wct = new WctEnricher( key.toString() );

		while( values.hasNext() ) {
			solr = values.next();
			wct.addWctMetadata( solr );
			mime = ( String ) solr.doc.getFieldValue( SolrFields.SOLR_NORMALISED_CONTENT_TYPE );
			try {
				if( mime.equals( "image" ) ) {
					this.solrImage.add( solr.doc );
				} else if( mime.equals( "media" ) ) {
					this.solrMedia.add( solr.doc );
				} else {
					this.solrDefault.add( solr.doc );
				}
			} catch( Exception e ) {
				e.printStackTrace();
			}
			// output.collect( new Text( "" ), new Text( ( String ) solr.doc.getFieldValue( SolrFields.SOLR_CONTENT_TYPE ) ) );
		}
	}

	@Override
	public void close() {
		try {
			this.solrDefault.flush();
			this.solrImage.flush();
			this.solrMedia.flush();
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}
}
