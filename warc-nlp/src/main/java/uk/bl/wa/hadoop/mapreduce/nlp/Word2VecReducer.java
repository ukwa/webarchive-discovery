package uk.bl.wa.hadoop.mapreduce.nlp;

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

import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.hadoop.mapreduce.mdx.MDXWritable;

@SuppressWarnings({ "deprecation" })
public class Word2VecReducer extends MapReduceBase implements
		Reducer<Text, MDXWritable, Text, Text> {

    private static Log log = LogFactory.getLog(Word2VecReducer.class);

	static enum MyCounters {
		NUM_RECORDS, NUM_REVISITS, NUM_ERRORS, NUM_DROPPED_RECORDS, NUM_UNRESOLVED_REVISITS, NUM_RESOLVED_REVISITS
	}

	private static final Text revisit = new Text("revisit");
	private static final Text response = new Text("response");

	public Word2VecReducer() {
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
	public void reduce(Text key, Iterator<MDXWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		long noValues = 0;
		MDXWritable mdx;
		MDX exemplar = null;
		List<MDXWritable> toReduplicate = new ArrayList<MDXWritable>();
		while (values.hasNext()) {
			mdx = values.next();
			noValues++;
			
			// Collect the revisit records:
			if (revisit.equals(mdx.getRecordType())) {
				// Add this revisit record to the stack:
				reporter.incrCounter(MyCounters.NUM_REVISITS, 1);
				toReduplicate.add(mdx);
			} else {
				// Record a response record:
				if (exemplar == null && response.equals(mdx.getRecordType())) {
					exemplar = mdx.getMDX();
				}
				// Collect complete records:
				Text outKey = new Text(mdx.getHash());
				output.collect(outKey, mdx.getMDXAsText());
			}
			
			// Report:
			reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

			// Occasionally update status report:
			if ((noValues % 1000) == 0) {
				reporter.setStatus("Processed "
						+ noValues
						+ ", of which "
						+ reporter.getCounter(MyCounters.NUM_REVISITS)
								.getValue() + " records need reduplication.");
			}

		}
		
		// Now fix up revisits:
		for (MDXWritable rmdxw : toReduplicate) {
			// Set outKey based on hash:
			Text outKey = rmdxw.getHash();
			// Handle merge:
			if( exemplar != null ) {
				// Modify record type and and merge the properties:
				MDX rmdx = rmdxw.getMDX();
				rmdx.setRecordType("reduplicated");
				rmdx.getProperties().putAll(exemplar.getProperties());
				reporter.incrCounter(MyCounters.NUM_RESOLVED_REVISITS, 1);
				// Collect resolved records:
				output.collect(outKey, new Text(rmdx.toJSON()));
			} else {
				reporter.incrCounter(MyCounters.NUM_UNRESOLVED_REVISITS, 1);
				// Collect unresolved records:
				output.collect(outKey, rmdxw.getMDXAsText());
			}
		}

	}

}