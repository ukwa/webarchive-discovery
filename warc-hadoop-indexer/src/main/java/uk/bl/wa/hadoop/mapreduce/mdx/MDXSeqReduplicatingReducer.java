package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;

@SuppressWarnings({ "deprecation" })
public class MDXSeqReduplicatingReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog(MDXSeqReduplicatingReducer.class);

	static enum MyCounters {
		NUM_RECORDS, NUM_ERRORS, NUM_DROPPED_RECORDS, NUM_UNRESOLVED_REVISITS, TO_REDUPLICATE, NUM_RESOLVED_REVISITS
	}

	public MDXSeqReduplicatingReducer() {
		try {
			Properties props = new Properties();
			props.load(getClass().getResourceAsStream(
					"/log4j-override.properties"));
			PropertyConfigurator.configure(props);
		} catch (IOException e1) {
			log.error("Failed to load log4j config from properties file.");
		}
	}

	/**
	 */
	@Override
	public void configure(JobConf job) {
		log.info("Initialisation complete.");
	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		long noValues = 0;
		Text map;
		MDX mdx, exemplar = null;
		List<MDX> toReduplicate = new ArrayList<MDX>();
		while (values.hasNext()) {
			map = values.next();
			noValues++;
			mdx = MDX.fromJSONString(map.toString());
			
			// Reformat the key:
			if( !"revisit".equals(mdx.getRecordType()) ) {
				if( "response".equals(mdx.getRecordType()) ) {
					exemplar = mdx;
				}
				// Collect complete records:
				Text outKey = new Text(mdx.getHash());
				output.collect(outKey, map);
			} else {
				// Report:
				reporter.incrCounter(MyCounters.TO_REDUPLICATE, 1);
				toReduplicate.add(mdx);
			}
			
			// Report:
			reporter.incrCounter(MyCounters.NUM_RECORDS, 1);
			// Occasionally update application-level status:
			if ((noValues % 1000) == 0) {
				reporter.setStatus("Processed "
						+ noValues
						+ ", dropped "
						+ reporter.getCounter(MyCounters.NUM_DROPPED_RECORDS)
						.getValue());	    
			}

		}
		
		// Now fix up revisits:
		for( MDX rmdx : toReduplicate ) {
			if( exemplar != null ) {
				// Modify record type and and merge the properties:
				rmdx.setRecordType("reduplicated");
				rmdx.getProperties().putAll(exemplar.getProperties());
				reporter.incrCounter(MyCounters.NUM_RESOLVED_REVISITS, 1);
			} else {
				reporter.incrCounter(MyCounters.NUM_UNRESOLVED_REVISITS, 1);
			}
			// Collect complete records:
			Text outKey = new Text(rmdx.getHash());
			output.collect(outKey, new Text(rmdx.toJSON()));
		}

	}

}