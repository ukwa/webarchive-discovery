package uk.bl.wa.hadoop.datasets;

/*
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import java.io.BufferedReader;
import java.io.File;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.mapred.FrequencyCountingReducer;
import uk.bl.wa.util.ConfigPrinter;

/**
 * 
 * Extracts metadata from a series of ARC/WARC files and formats the results
 * into datasets.
 * 
 * @author anjackson
 */

@SuppressWarnings({ "deprecation" })
public class WARCDatasetGenerator extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WARCDatasetGenerator.class);
    private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-c <config file>] [-d] [Dump config.] [-w] [Wait for completion.] [-x] [output XML in OAI-PMH format]";
    private static final String CLI_HEADER = "WARCDatasetGenerator - MapReduce method for extracing datasets from ARCs and WARCs";
    public static final String CONFIG_PROPERTIES = "warc_indexer_config";

    private String inputPath;
    private String outputPath;
    private String configPath;
    private boolean wait;
    private boolean dumpConfig;

    public static String FORMATS_SUMMARY_NAME = "formats";
    public static String FORMATS_FFB_NAME = "formatsExt";
    public static String HOSTS_NAME = "hosts";
    public static String HOST_LINKS_NAME = "hostLinks";
    public static String FACES_NAME = "faces";
    public static String GEO_SUMMARY_NAME = "geo";

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
        // Decide whether to apply annotations:
        // Store the properties:
        conf.set(CONFIG_PROPERTIES, index_conf.withOnlyPath("warc").root()
                .render(ConfigRenderOptions.concise()));
        LOG.info("Loaded warc config.");
        LOG.info(index_conf.getString("warc.title"));

        // Reducer count
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
        conf.setMapperClass(WARCDatasetMapper.class);
        conf.setReducerClass(FrequencyCountingReducer.class);
        // This can be optionally use to suppress keys:
        // conf.setOutputFormat(KeylessTextOutputFormat.class);
        // conf.set( "map.output.key.field.separator", "" );

        // Compress the output from the maps, to cut down temp space
        // requirements between map and reduce.
        conf.setBoolean("mapreduce.map.output.compress", true); // Wrong syntax
        // for 0.20.x ?
        conf.set("mapred.compress.map.output", "true");
        // conf.set("mapred.map.output.compression.codec",
        // "org.apache.hadoop.io.compress.GzipCodec");
        // Ensure the JARs we provide take precedence over ones from Hadoop:
        conf.setBoolean("mapreduce.task.classpath.user.precedence", true);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setNumReduceTasks(numReducers);

        MultipleOutputs.addMultiNamedOutput(conf, FORMATS_SUMMARY_NAME,
                TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, FORMATS_FFB_NAME,
                TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, HOSTS_NAME,
                TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, HOST_LINKS_NAME,
                TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, GEO_SUMMARY_NAME,
                TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, FACES_NAME,
                TextOutputFormat.class, Text.class, Text.class);

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
        JobConf conf = new JobConf(getConf(), WARCDatasetGenerator.class);

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
        options.addOption("d", false, "dump configuration");
        options.addOption("h", false, "print help");
        options.addOption("i", true, "input file list");
        options.addOption("o", true, "output directory");
        options.addOption("c", true, "path to configuration");
        options.addOption("w", false, "wait for job to finish");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, otherArgs);
        this.inputPath = cmd.getOptionValue("i");
        this.outputPath = cmd.getOptionValue("o");
        this.wait = cmd.hasOption("w");
        if (cmd.hasOption("c")) {
            this.configPath = cmd.getOptionValue("c");
        }
        this.dumpConfig = cmd.hasOption("d");

        // If we are just dumping the config no need to validate:
        if (this.dumpConfig)
            return;

        // If we are prining help, just do that:
        if (cmd.hasOption('h'))
            printHelp("", options);

        // Validate remaining args:
        if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
            printHelp(
                    "\nERROR: You must specify both the input and output parameters!",
                    options);
        }

    }

    private void printHelp(String message, Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(80);
        helpFormatter.printHelp(CLI_USAGE, CLI_HEADER, options, message);
        System.out.println("\n");
        ToolRunner.printGenericCommandUsage(System.out);
        System.exit(1);
    }

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WARCDatasetGenerator(), args);
        System.exit(ret);
    }

}
