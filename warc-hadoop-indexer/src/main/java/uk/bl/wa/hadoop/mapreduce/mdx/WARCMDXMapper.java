package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.apache.solr.hadoop.Solate;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@SuppressWarnings( { "deprecation" } )
public class WARCMDXMapper extends MapReduceBase implements
		Mapper<Text, WritableArchiveRecord, Text, Text> {
	private static final Log LOG = LogFactory.getLog( WARCMDXMapper.class );

	static enum MyCounters {
		NUM_RECORDS, NUM_ERRORS, NUM_NULLS, NUM_EMPTY_HEADERS
	}

	private String mapTaskId;
	private String inputFile;
	private int noRecords = 0;

	private WARCIndexer windex;

	private Solate sp = null;
	private int numShards = 1;
	private Config config;

	public WARCMDXMapper() {
		try {
			// Re-configure logging:
			Properties props = new Properties();
			props.load(getClass().getResourceAsStream("/log4j-override.properties"));
			PropertyConfigurator.configure(props);
		} catch (IOException e1) {
			LOG.error("Failed to load log4j config from properties file.");
		}
	}

	public WARCMDXMapper(JobConf conf) {
		this.configure(conf);
	}

	@Override
	public void configure( JobConf job ) {
		// Get config from job property:
		if (job.get(WARCMDXGenerator.CONFIG_PROPERTIES) != null) {
			config = ConfigFactory.parseString(job
					.get(WARCMDXGenerator.CONFIG_PROPERTIES));
		} else {
			config = ConfigFactory.load();
		}
		// Initialise indexer:
		if (this.windex == null) {
			try {
				this.windex = new WARCIndexer(config);
			} catch (NoSuchAlgorithmException e) {
				LOG.error("Error setting up WARC Indexer: " + e, e);
			}
		} else {
			LOG.info("Indexer has already been initialised...");
		}
		// Other properties:
		mapTaskId = job.get("mapred.task.id");
		inputFile = job.get("map.input.file");
		LOG.info("Got task.id " + mapTaskId + " and input.file "
					+ inputFile);
	}

	@Override
	public void map(Text key, WritableArchiveRecord value,
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();

		noRecords++;

		ArchiveRecord rec = value.getRecord();
		SolrRecord solr = new SolrRecord(key.toString(), rec.getHeader());
		try {
			if (!header.getHeaderFields().isEmpty()) {
				// Do the indexing:
				solr = windex.extract(key.toString(),
						value.getRecord());

				// If there is no result, report it
				if (solr == null) {
					LOG.debug("WARCIndexer returned NULL for "
							+ header.getMimetype() + ": "
							+ header.getUrl());
					reporter.incrCounter(MyCounters.NUM_NULLS, 1);
					return;
				}

				// String host = (String)
				// solr.getFieldValue(SolrFields.SOLR_HOST);
				// if (host == null) {
				// host = "unknown.host";
				// }

				// Increment record counter:
				reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

			} else {
				// Report headerless records:
				reporter.incrCounter(MyCounters.NUM_EMPTY_HEADERS, 1);

			}

		} catch (Exception e) {
			LOG.error(e.getClass().getName() + ": " + e.getMessage() + "; "
					+ header.getUrl() + "; " + header.getOffset());
			// Increment error counter
			reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
			// Store it:
			solr.addParseException(e);

		} catch (OutOfMemoryError e) {
			// Allow processing to continue if a record causes OOME:
			LOG.error("OOME " + e.getClass().getName() + ": " + e.getMessage()
					+ "; " + header.getUrl() + "; " + header.getOffset());
			// Increment error counter
			reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
			// Store it:
			solr.addParseException(e);
		}

		// Strip out text:
		solr.removeField(SolrFields.SOLR_EXTRACTED_TEXT);
		solr.removeField(SolrFields.SOLR_EXTRACTED_TEXT_NOT_STORED);

		// Wrap up and collect the result:
		String hash = (String) solr.getFieldValue(SolrFields.HASH);
		if (hash != null) {
			Text oKey = new Text(hash);
			MDX mdx = MDX.fromWritabelSolrRecord(solr);
			Text result = new Text(mdx.toJSON());
			output.collect(oKey, result);
		} else {
			LOG.warn("Hash is null for " + header.getMimetype() + " - "
					+ header.getUrl() + " "
					+ header.getReaderIdentifier());
		}

		// Occasionally update application-level status
		if ((noRecords % 1000) == 0) {
			reporter.setStatus(noRecords + " processed from " + inputFile);
			// Also assure framework that we are making progress:
			reporter.progress();
		}

	}


}
