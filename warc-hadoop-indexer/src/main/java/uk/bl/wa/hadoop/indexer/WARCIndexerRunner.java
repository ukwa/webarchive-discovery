package uk.bl.wa.hadoop.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.util.ConfigPrinter;

import com.sun.syndication.io.impl.Base64;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

/**
 * WARCIndexerRunner Extracts text/metadata using from a series of Archive
 * files.
 * 
 * @author rcoram
 */

@SuppressWarnings({ "deprecation" })
public class WARCIndexerRunner extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(WARCIndexerRunner.class);
    private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-c <config file>] [-a] [Read from ACT.] [-d] [Dump config.] [-w] [Wait for completion.]";
    private static final String CLI_HEADER = "WARCIndexerRunner - MapReduce method for extracing metadata/text from Archive Records";
    public static final String CONFIG_PROPERTIES = "warc_indexer_config";

    private String inputPath;
    private String outputPath;
    private String configPath;
    private boolean readAct;
    private String actFile;
    private boolean wait;
    private boolean dumpConfig;

    private String cookie;
    private String csrf;

    /**
     * Performs login operation to ACT, setting Cookie and CSRF.
     * 
     * @throws IOException
     */
    protected void actLogin() throws IOException {
	Config loginConf = ConfigFactory.load("credentials.conf");
	URL login = new URL(loginConf.getString("act.login"));

	HttpURLConnection connection = (HttpURLConnection) login
		.openConnection();
	StringBuilder credentials = new StringBuilder();
	credentials.append(loginConf.getString("act.username"));
	credentials.append(":");
	credentials.append(loginConf.getString("act.password"));
	connection.setRequestProperty("Authorization",
		"Basic " + Base64.encode(credentials.toString()));

	Scanner scanner;
	if (connection.getResponseCode() != 200) {
	    scanner = new Scanner(connection.getErrorStream());
	    scanner.useDelimiter("\\Z");
	    throw new IOException(scanner.next());
	} else {
	    scanner = new Scanner(connection.getInputStream());
	}
	scanner.useDelimiter("\\Z");
	this.csrf = scanner.next();
	this.cookie = connection.getHeaderField("set-cookie");
    }

    /**
     * 
     * @param args
     * @return
     * @throws IOException
     * @throws ParseException
     */
    protected void createJobConf(JobConf conf, String[] args)
	    throws IOException, ParseException {
	// Parse the command-line parameters.
	this.setup(args);

	// Store application properties where the mappers/reducers can access
	// them
	Config index_conf;
	if (this.configPath != null) {
	    index_conf = ConfigFactory.parseFile(new File(this.configPath));
	} else {
	    index_conf = ConfigFactory.load();
	}
	if (this.dumpConfig) {
	    ConfigPrinter.print(index_conf);
	    System.exit(0);
	}
	conf.set(CONFIG_PROPERTIES, index_conf.withOnlyPath("warc").root()
		.render(ConfigRenderOptions.concise()));
	LOG.info("Loaded warc config.");
	LOG.info(index_conf.getString("warc.title"));
	if (this.actFile != null) {
	    LOG.info("ACT file: " + this.actFile);
	} else {
	    LOG.info("ACT URL: " + index_conf.getString("warc.act.url"));
	}
	LOG.info("ACT Collection URL: "
		+ index_conf.getString("warc.act.collections.url"));
	if (index_conf.getBoolean("warc.solr.use_hash_url_id")) {
	    LOG.info("Using hash-based ID.");
	}
	if (index_conf.hasPath("warc.solr.zookeepers")) {
	    LOG.info("Using Zookeepers.");
	} else {
	    LOG.info("Using SolrServers.");
	}

	// Pull in ACT metadata
	if (this.readAct) {
	    LOG.info("Getting ACT credentials...");
	    this.actLogin();
	    LOG.info("Reading Collections from ACT...");
	    conf.set("warc.act.collections.xml",
		    readAct(index_conf.getString("warc.act.collections.url")));
	    if (this.actFile != null) {
		LOG.info("Reading ACT records from file...");
		conf.set("warc.act.xml",
			FileUtils.readFileToString(new File(this.actFile)));
	    } else {
		LOG.info("Reading records from ACT...");
		conf.set("warc.act.xml",
			readAct(index_conf.getString("warc.act.url")));
	    }
	    LOG.info("Read " + conf.get("warc.act.xml").length() + " bytes.");
	}
	// Also set reduce speculative execution off, avoiding duplicate
	// submissions to Solr.
	conf.set("mapred.reduce.tasks.speculative.execution", "false");

	// Reducer count dependent on concurrent HTTP connections to Solr
	// server.
	int numReducers = 1;
	try {
	    numReducers = index_conf.getInt("warc.hadoop.num_reducers");
	} catch (NumberFormatException n) {
	    numReducers = 10;
	}

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
	conf.setInputFormat(ArchiveFileInputFormat.class);
	conf.setMapperClass(WARCIndexerMapper.class);
	conf.setReducerClass(WARCIndexerReducer.class);
	conf.setOutputFormat(TextOutputFormat.class);
	conf.set("map.output.key.field.separator", "");
	// Compress the output from the maps, to cut down temp space
	// requirements between map and reduce.
	conf.setBoolean("mapreduce.map.output.compress", true);
	// Ensure the JARs we provide take precedence over ones from Hadoop:
	// conf.setBoolean("mapreduce.task.classpath.user.precedence", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapOutputValueClass(WritableSolrRecord.class);
	conf.setNumReduceTasks(numReducers);
    }

    /**
     * Read data from ACT to include curator-specified metadata.
     * 
     * @param conf
     * @return
     * @throws MalformedURLException
     * @throws IOException
     */
    protected String readAct(String url) throws IOException {
	URL act = new URL(url);
	HttpURLConnection connection = (HttpURLConnection) act.openConnection();
	if (this.cookie != null) {
	    connection.setRequestProperty("Cookie", this.cookie);
	    connection.setRequestProperty("X-CSRF-TOKEN", this.csrf);
	}

	Scanner scanner;
	if (connection.getResponseCode() != 200) {
	    scanner = new Scanner(connection.getErrorStream());
	    scanner.useDelimiter("\\Z");
	    throw new IOException(scanner.next());
	} else {
	    scanner = new Scanner(connection.getInputStream());
	}
	scanner.useDelimiter("\\Z");
	return scanner.next();
    }

    /**
     * 
     * Run the job:
     * 
     */
    public int run(String[] args) throws IOException, ParseException {
	// Set up the base conf:
	JobConf conf = new JobConf(getConf(), WARCIndexerRunner.class);

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

    private void setup(String[] args) throws ParseException {
	Options options = new Options();
	options.addOption("i", true, "input file list");
	options.addOption("o", true, "output directory");
	options.addOption("c", true, "path to configuration");
	options.addOption("a", false, "read data from ACT");
	options.addOption("f", true, "read ACT records from file");
	options.addOption("w", false, "wait for job to finish");
	options.addOption("d", false, "dump configuration");
	// TODO: Problematic with "hadoop jar"?
	// options.addOption( OptionBuilder.withArgName( "property=value"
	// ).hasArgs( 2 ).withValueSeparator().withDescription(
	// "use value for given property" ).create( "D" ) );

	CommandLineParser parser = new PosixParser();
	CommandLine cmd = parser.parse(options, args);
	if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
	    HelpFormatter helpFormatter = new HelpFormatter();
	    helpFormatter.setWidth(80);
	    helpFormatter.printHelp(CLI_USAGE, CLI_HEADER, options, "");
	    System.exit(1);
	}
	this.inputPath = cmd.getOptionValue("i");
	this.outputPath = cmd.getOptionValue("o");
	this.readAct = cmd.hasOption("a");
	this.actFile = cmd.getOptionValue("f");
	this.wait = cmd.hasOption("w");
	if (cmd.hasOption("c")) {
	    this.configPath = cmd.getOptionValue("c");
	}
	this.dumpConfig = cmd.hasOption("d");
    }

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new WARCIndexerRunner(), args);
	System.exit(ret);
    }

}
