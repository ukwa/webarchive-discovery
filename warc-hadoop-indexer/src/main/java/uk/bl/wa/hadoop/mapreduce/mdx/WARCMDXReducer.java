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
public class WARCMDXReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog(WARCMDXReducer.class);

	static enum MyCounters {
		NUM_RECORDS, NUM_ERRORS, NUM_DROPPED_RECORDS, NUM_UNRESOLVED_REVISITS
	}

	public WARCMDXReducer() {
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
	 * Sets up our SolrServer. Presumes the existence of either
	 * "warc.solr.zookepers" or "warc.solr.servers" in the config.
	 */
	@Override
	public void configure(JobConf job) {
		log.info("Initialisation complete.");
	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		// Go through the documents for this shard:
		long noValues = 0;
		Text map;
		MDX mdx;
		List<MDX> revisits = new ArrayList<MDX>();
		List<Text> mdxs = new ArrayList<Text>();
		while (values.hasNext()) {
			map = values.next();
			noValues++;
			mdx = MDX.fromJSONString(map.toString());
			
			if( "revist".equals(mdx.getRecordType())) {
				revisits.add(mdx);
				log.info("Resolving... " + key + " " + map);
			} else {
				mdxs.add(map);
				log.info("Recording... " + key + " " + map);
			}
			
		}
		
		// Merge/resolve:
		if (mdxs.size() == 0) {
			log.warn("Could not resolve revists for " + key);
			reporter.incrCounter(MyCounters.NUM_UNRESOLVED_REVISITS, 1);
		} else {
			for (MDX rmdx : revisits) {
				// FIXME Add a reconstituted MDX to the output:
				rmdx.getHash();
			}
		}

		// Now collect...
		for (Text tmdx : mdxs) {

			// Post-process:
			output.collect(key, tmdx);
			
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

	}

}