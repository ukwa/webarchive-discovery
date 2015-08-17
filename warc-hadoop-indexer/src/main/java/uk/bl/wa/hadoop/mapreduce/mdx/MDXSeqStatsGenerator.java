package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.hadoop.mapreduce.FrequencyCountingReducer;
import uk.bl.wa.solr.SolrFields;

/**
 * WARCIndexerRunner
 * 
 * Extracts text/metadata using from a series of Archive files.
 * 
 * @author rcoram
 */

@SuppressWarnings({ "deprecation" })
public class MDXSeqStatsGenerator extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(MDXSeqStatsGenerator.class);
	private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-c <config file>] [-d] [Dump config.] [-w] [Wait for completion.]";
	private static final String CLI_HEADER = "MapReduce job extracting data from MDX Sequence Files.";

	private String inputPath;
	private String outputPath;
	private boolean wait;

	public static String FORMATS_SUMMARY_NAME = "fmts";
	public static String FORMATS_FFB_NAME = "fmtsffb";
	public static String HOST_LINKS_NAME = "hostlinks";
	public static String GEO_NAME = "geo";
	public static String KEY_PREFIX = "__";


	/**
	 * 
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	protected void createJobConf(JobConf conf, String[] args)
			throws IOException, ParseException, KeeperException,
			InterruptedException {
		// Parse the command-line parameters.
		this.setup(args, conf);

		// Reducer count:
		int numReducers = 1;

		// Add input paths:
		LOG.info("Reading input files...");
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader(this.inputPath));
		while ((line = br.readLine()) != null) {
			FileInputFormat.addInputPath(conf, new Path(line));
		}
		br.close();
		LOG.info("Read " + FileInputFormat.getInputPaths(conf).length
				+ " input files.");

		FileOutputFormat.setOutputPath(conf, new Path(this.outputPath));

		conf.setJobName(this.inputPath + "_" + System.currentTimeMillis());
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapperClass(MDXSeqStatsMapper.class);
		conf.setReducerClass(FrequencyCountingReducer.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setNumReduceTasks(numReducers);
		
		MultipleOutputs.addMultiNamedOutput(conf, FORMATS_SUMMARY_NAME,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addMultiNamedOutput(conf, FORMATS_FFB_NAME,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addMultiNamedOutput(conf, HOST_LINKS_NAME,
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addMultiNamedOutput(conf, GEO_NAME,
				TextOutputFormat.class, Text.class, Text.class);

		TextOutputFormat.setCompressOutput(conf, true);
		TextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}

	/**
	 * 
	 * Run the job:
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 * 
	 */
	public int run(String[] args) throws IOException, ParseException,
			KeeperException, InterruptedException {
		// Set up the base conf:
		JobConf conf = new JobConf(getConf(), MDXSeqStatsGenerator.class);

		// Get the job configuration:
		this.createJobConf(conf, args);

		// Submit it:
		if (this.wait) {
			JobClient.runJob(conf);
		} else {
			JobClient client = new JobClient(conf);
			client.submitJob(conf);
		}
		return 0;
	}

	private void setup(String[] args, JobConf conf) throws ParseException {
		// Process Hadoop args first:
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// Process remaining args list this:
		Options options = new Options();
		options.addOption("i", true, "input file list");
		options.addOption("o", true, "output directory");
		options.addOption("w", false, "wait for job to finish");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, otherArgs);
		if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.setWidth(80);
			helpFormatter.printHelp(CLI_USAGE, CLI_HEADER, options, "");
			System.exit(1);
		}
		this.inputPath = cmd.getOptionValue("i");
		this.outputPath = cmd.getOptionValue("o");
		this.wait = cmd.hasOption("w");
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MDXSeqStatsGenerator(), args);
		System.exit(ret);
	}

	/**
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 *
	 */
	static public class MDXSeqStatsMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private boolean scanFormats = true;
		private boolean scanHostLinks = true;

		private static String getFirstOrNull(List<String> list) {
			if (list == null || list.isEmpty()) {
				return "null";
			} else {
				return list.get(0);
			}
		}

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// Parse the MDX:
			MDX mdx = MDX.fromJSONString(value.toString());
			Map<String, List<String>> p = mdx.getProperties();
			String year = mdx.getTs().substring(0, 4);
			if (!"request".equals(mdx.getRecordType())) {
				// Generate format summary:
				if (scanFormats) {
					String cts = getFirstOrNull(p
							.get(SolrFields.CONTENT_TYPE_SERVED));
					String ctt = getFirstOrNull(p
							.get(SolrFields.CONTENT_TYPE_TIKA));
					String ctd = getFirstOrNull(p
							.get(SolrFields.CONTENT_TYPE_DROID));
					output.collect(new Text(FORMATS_SUMMARY_NAME + KEY_PREFIX
							+ year), new Text(year + "\t" + cts + "\t" + ctt
							+ "\t" + ctd));

					String ct = getFirstOrNull(p
							.get(SolrFields.SOLR_CONTENT_TYPE));
					String ctext = getFirstOrNull(p
							.get(SolrFields.CONTENT_TYPE_EXT));
					String ctffb = getFirstOrNull(p.get(SolrFields.CONTENT_FFB));
					output.collect(new Text(FORMATS_FFB_NAME + KEY_PREFIX
							+ year), new Text(year + "\t" + ct + "\t" + ctext
							+ "\t" + ctffb));
				}
				// Generate host link graph
				if (scanHostLinks) {
					String host = getFirstOrNull(p.get(SolrFields.SOLR_HOST));
					List<String> hosts = p.get(SolrFields.SOLR_LINKS_HOSTS);
					if (hosts != null) {
						for (String link_host : hosts) {
							String link = host + "\t" + link_host;
							output.collect(new Text(HOST_LINKS_NAME
									+ KEY_PREFIX + year), new Text(year + "\t"
									+ link));
						}
					} else {
						// TBA Reporter that hosts was null;
					}

				}
				// Now look for postcodes:
				List<String> postcodes = p.get(SolrFields.POSTCODE);
				List<String> locations = p.get(SolrFields.LOCATIONS);
				if (postcodes != null) {
					for (int i = 0; i < postcodes.size(); i++) {
						String location = "";
						if (locations != null
								&& locations.size() == postcodes.size()) {
							location = locations.get(i);
						}
						String result = mdx.getTs() + "/" + mdx.getUrl() + "\t"
								+ postcodes.get(i) + "\t" + locations.get(i);
						output.collect(new Text(GEO_NAME + KEY_PREFIX + year),
								new Text(result));
					}
				}
			} else {
				// TBA reporter to say how many request records ignored.
			}
		}

	}

}
