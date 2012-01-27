package uk.bl.wap.hadoop.tika;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import uk.bl.wap.util.solr.SolrRecord;
import uk.bl.wap.util.solr.WctEnricher;

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaReducer extends MapReduceBase implements Reducer<Text, SolrRecord, Text, Text> {
	public ArchiveTikaReducer() {}

	@Override
	public void reduce( Text key, Iterator<SolrRecord> values, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		SolrRecord sr = new SolrRecord();
		WctEnricher wct = new WctEnricher( key.toString() );

		output.collect( new Text( "" ), new Text( "<?xml version=\"1.0\" encoding=\"UTF-8\"?><wctdocs>" ) );

		while( values.hasNext() ) {
			sr = values.next();
			wct.populate( sr );
			output.collect( new Text( "" ), new Text( sr.toXml() ) );
		}
		output.collect( new Text( "" ), new Text( "</wctdocs>" ) );
	}
}
