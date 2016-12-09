package uk.bl.wa.hadoop.indexer.mdx;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.random.RandomDataGenerator;
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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.solr.SolrFields;

/**
 * WARCIndexerRunner
 * 
 * Extracts text/metadata using from a series of Archive files.
 * 
 * @author rcoram
 */

@SuppressWarnings({ "deprecation" })
public class MDXSeqSampleGenerator extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(MDXSeqSampleGenerator.class);
	private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-r <#reducers>] [-w] [Wait for completion.]";
	private static final String CLI_HEADER = "MapReduce job extracting data from MDX Sequence Files.";

	private String inputPath;
	private String outputPath;
	private boolean wait;

	public static String GEO_NAME = "geoSample";
	public static String FORMATS_FFB_SAMPLE_NAME = "formatsExtSample";
	public static String KEY_PREFIX = "__";

	// Reducer count:
	private int numReducers = 1;

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
		conf.setMapperClass(MDXSeqSampleMapper.class);
		conf.setReducerClass(ReservoirSamplingReducer.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setNumReduceTasks(numReducers);
		
		MultipleOutputs.addMultiNamedOutput(conf, GEO_NAME,
				TextOutputFormat.class, Text.class, Text.class);

		MultipleOutputs.addMultiNamedOutput(conf, FORMATS_FFB_SAMPLE_NAME,
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
		JobConf conf = new JobConf(getConf(), MDXSeqSampleGenerator.class);

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
		options.addOption("r", true, "number of reducers");

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
		if (cmd.hasOption("r")) {
			this.numReducers = Integer.parseInt(cmd.getOptionValue("r"));
		}
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MDXSeqSampleGenerator(), args);
		System.exit(ret);
	}

	/**
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 *
	 */
	static public class MDXSeqSampleMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		static String getFirstOrNull(List<String> list) {
			if (list == null || list.isEmpty()) {
				return "";
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
			String year_month = year;
			if (mdx.getTs().length() >= 6) {
				year_month = mdx.getTs().substring(0, 6);
			} else {
				year_month = year + "xx";
			}
			if (!"request".equals(mdx.getRecordType())) {
				// Look for postcodes and locations:
				List<String> postcodes = p.get(SolrFields.POSTCODE);
				List<String> locations = p.get(SolrFields.LOCATIONS);
				if (postcodes != null) {
					for (int i = 0; i < postcodes.size(); i++) {
						String location = "";
						if (locations != null
								&& locations.size() == postcodes.size()) {
							location = locations.get(i);
						} else {
							// Reporter;
							reporter.incrCounter("MDX-Records",
									"Unresolved-Locations", 1);
						}
						// Full geo-index
						String result = mdx.getTs() + "/" + mdx.getUrl() + "\t"
								+ postcodes.get(i) + "\t" + location;
						output.collect(new Text(GEO_NAME + KEY_PREFIX
								+ year_month), new Text(result));
					}
				}
				// Look for examples from formats
				String ct = getFirstOrNull(p.get(SolrFields.SOLR_CONTENT_TYPE));
				String ctext = getFirstOrNull(p
						.get(SolrFields.CONTENT_TYPE_EXT));
				String ctffb = getFirstOrNull(p.get(SolrFields.CONTENT_FFB));
				output.collect(new Text(FORMATS_FFB_SAMPLE_NAME + KEY_PREFIX
						+ year),
						new Text(year + "\t" + ct + "\t" + ctext + "\t" + ctffb
								+ "\t" + mdx.getTs() + "/" + mdx.getUrl()));

			} else {
				// Reporter;
				reporter.incrCounter("MDX-Records",
						"Ignored-" + mdx.getRecordType() + "-Record", 1);
			}
		}

	}
	
	/**
	 * This reservoir-sampling reducer selects a finite random subset of any
	 * stream of values passed to it.
	 * 
	 * Defaults to 1000 items in the sample.
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 *
	 */
	static public class ReservoirSamplingReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		private int numSamples = 1000;
		private long defaultSeed = 1231241245l;
		private MultipleOutputs mos;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop
		 * .mapred .JobConf)
		 */
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			mos = new MultipleOutputs(job);
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Text item;
			long numItemsSeen = 0;
			Vector<Text> reservoir = new Vector<Text>();
			RandomDataGenerator random = new RandomDataGenerator();
			// Fix the seed so repoducible by default:
			random.reSeed(defaultSeed);

			// Iterate through all values:
			while (values.hasNext()) {
				item = values.next();

				if (reservoir.size() < numSamples) {
					// reservoir not yet full, just append
					reservoir.add(item);
				} else {
					// find a sample to replace
					long rIndex = random.nextLong(0, numItemsSeen);
					if (rIndex < numSamples) {
						reservoir.set((int) rIndex, item);
					}
				}
				numItemsSeen++;
			}

			// Choose the output:
			Text outKey = key;
			OutputCollector<Text, Text> collector;
			int pos = key.find("__");
			if (pos == -1) {
				collector = output;
			} else {
				String[] fp = key.toString().split("__");
				collector = getCollector(fp[0], fp[1], reporter);
				outKey = new Text(fp[1]);
			}

			// Now output the sample:
			for (Text sto : reservoir) {
				collector.collect(outKey, sto);
			}
		}

		@SuppressWarnings("unchecked")
		private OutputCollector<Text, Text> getCollector(String fp, String fp2,
				Reporter reporter) throws IOException {
			return mos.getCollector(fp, fp2, reporter);
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

}
