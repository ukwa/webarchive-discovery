package uk.bl.wap.hadoop.regex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings( { "deprecation" } )
public class WARCRegexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce( Text key, Iterator<Text> iterator, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		while( iterator.hasNext() ) {
			output.collect( key, iterator.next() );
		}
	}
}
