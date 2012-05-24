package uk.bl.wap.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings( { "deprecation" } )
/**
 * 
 */
public class FrequencyCountingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce( Text key, Iterator<Text> iterator, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		
		// Use a simple set to collect only distinct results for this key:
		Map<String,MutableInt> matches = new HashMap<String,MutableInt>();
		while( iterator.hasNext() ) {
			String m = iterator.next().toString();
			// Get or set up the counter:
			MutableInt value = matches.get(m);
			if( value == null ) {
				value = new MutableInt();
				matches.put(m, value);
			}
			// Increment the counter for this match:
			value.inc();
	}
		
		// Loop through and collect all distinct matches:
		for( String match : matches.keySet() ) {
			// This ignores the count:
			//output.collect( key, new Text( match ) );
			// This also outputs the count:
			output.collect( key, new Text( match + "\t" + matches.get(match).get() ) );
		}
		
	}
}
