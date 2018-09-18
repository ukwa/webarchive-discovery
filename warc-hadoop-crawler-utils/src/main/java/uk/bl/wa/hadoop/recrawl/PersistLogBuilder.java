package uk.bl.wa.hadoop.recrawl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.TextOutputFormat;

@SuppressWarnings("deprecation")
public class PersistLogBuilder extends Configured implements Tool {
    private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-r <number of reducers>]";
    private static final String CLI_HEADER = "PersistLogBuilder - MapReduce method for building persistlog data from WARCS.";
    private String input;
    private String output;
    private int reducers = 1;

    public int run(String[] args) throws IOException, ParseException {
    JobConf conf = new JobConf(getConf(), PersistLogBuilder.class);
    String line = null;
    setup(args);

    BufferedReader br = new BufferedReader(new FileReader(this.input));

    int lineCount = 0;
    while ((line = br.readLine()) != null) {
        FileInputFormat.addInputPath(conf, new Path(line));
        System.out.print("Added " + ++lineCount + " input paths.\r");
    }
    System.out.println();

    FileOutputFormat.setOutputPath(conf, new Path(this.output));
    conf.setJobName(this.input + "_" + System.currentTimeMillis());
    conf.setInputFormat(ArchiveFileInputFormat.class);
    conf.setMapperClass(PersistLogMapper.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setNumReduceTasks(reducers);
    this.setProperties(conf);
    JobClient client = new JobClient(conf);
    client.submitJob(conf);
    return 0;
    }

    private void setup(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("i", true, "input file list");
    options.addOption("o", true, "output directory");
    options.addOption("r", true, "number of reducers");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(80);
        helpFormatter.printHelp(CLI_USAGE, CLI_HEADER, options, "");
        System.exit(1);
    }
    this.input = cmd.getOptionValue("i");
    this.output = cmd.getOptionValue("o");
    if (cmd.hasOption("r")) {
        reducers = Integer.parseInt(cmd.getOptionValue("r"));
    }
    }

    public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new PersistLogBuilder(), args);
    System.exit(ret);
    }

    private void setProperties(JobConf conf) throws IOException {
    conf.set("mapred.reduce.tasks.speculative.execution", "false");
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.compress.map.output", "true");
    conf.setClass("mapred.output.compression.codec", GzipCodec.class,
        CompressionCodec.class);
    }
}
