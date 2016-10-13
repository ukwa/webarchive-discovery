package uk.bl.wa.hadoop.mapreduce.nlp;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.hadoop.indexer.WARCIndexerMapper;
import uk.bl.wa.hadoop.indexer.WritableSolrRecord;
import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.hadoop.mapreduce.mdx.MDXWritable;
import uk.bl.wa.solr.SolrRecord;

@SuppressWarnings( { "deprecation" } )
public class WARCSentenceMapper extends MapReduceBase implements
		Mapper<Text, WritableArchiveRecord, Text, MDXWritable> {
    private static final Log LOG = LogFactory.getLog(WARCSentenceMapper.class);

	private WARCIndexerMapper wim;

	public WARCSentenceMapper() {
		try {
			// Re-configure logging:
			Properties props = new Properties();
			props.load(getClass().getResourceAsStream("/log4j-override.properties"));
			PropertyConfigurator.configure(props);
		} catch (IOException e1) {
			LOG.error("Failed to load log4j config from properties file.");
		}
	}

	@Override
	public void configure(JobConf job) {
		if (wim == null) {
			wim = new WARCIndexerMapper();
			wim.configure(job);
		}
	}

	@Override
	public void map(Text key, WritableArchiveRecord value,
			OutputCollector<Text, MDXWritable> output,
			Reporter reporter) throws IOException {

		// Use the main indexing code:
		WritableSolrRecord wsolr = wim.innerMap(key, value, reporter);

		// Ignore skipped records, where wsolr will be NULL:
		if (wsolr != null) {
			SolrRecord solr = wsolr.getSolrRecord();

			// Wrap up the result:
			MDX mdx = MDX.fromWritableSolrRecord(solr);
			// Wrap up the key:
			Text oKey = new Text(mdx.getHash());
			// Alternative key, based on record type + url + timestamp
			// Text oKey = new Text(mdx.getUrl() + "\t" + mdx.getTs() + "\t"
			// + mdx.getRecordType());

			// Collect
			MDXWritable result = new MDXWritable(mdx);
			output.collect(oKey, result);
		}

	}


}
