package uk.bl.wa.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.log4j.Logger;

import uk.bl.wa.hadoop.mapreduce.MutableInt;


@SuppressWarnings( { "deprecation" } )
/**
 * 
 * This counts the number of times each Value appears for this Key. Then outputs as:
 * 
 *     Key Value Count
 * 
 */
public class FrequencyCountingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private static Logger log = Logger
			.getLogger(FrequencyCountingReducer.class);
	
	private MultipleOutputs mos;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
	 * .JobConf)
	 */
	@Override
	public void configure(JobConf job) {
		super.configure(job);
		mos = new MultipleOutputs(job);
	}

	@Override
	public void reduce( Text key, Iterator<Text> iterator, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		
		log.warn("Reducing for key: " + key);

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
		Text result = new Text();
		Text outKey = key;
		OutputCollector<Text, Text> collector;
		int pos = key.find("__");
		if (pos == -1) {
			collector = output;
		} else {
			String[] fp = key.toString().split("__");
			collector = mos.getCollector(fp[0], fp[1], reporter);
			outKey = new Text(fp[1]);
		}
		log.info("For key: " + key + " outKey " + outKey + " outputting "
				+ matches.size() + " unique values.");
		for( String match : matches.keySet() ) {
			// This outputs the count:
			result.set(match + "\t" + matches.get(match).get());
			// And collect:
			collector.collect(outKey, result);
		}
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapred.MapReduceBase#close()
	 */
	@Override
	public void close() throws IOException {
		super.close();
		mos.close();
	}

}
