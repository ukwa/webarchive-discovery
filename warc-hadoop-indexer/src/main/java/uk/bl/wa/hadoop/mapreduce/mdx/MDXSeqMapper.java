package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Simple mapper class to turn MDX-as-Text into MDXWritables.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDXSeqMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, MDXWritable> {

	public MDXSeqMapper() {
	}

	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, MDXWritable> output, Reporter reporter)
			throws IOException {
		output.collect(value,
				new MDXWritable(MDX.fromJSONString(value.toString())));
	}

}