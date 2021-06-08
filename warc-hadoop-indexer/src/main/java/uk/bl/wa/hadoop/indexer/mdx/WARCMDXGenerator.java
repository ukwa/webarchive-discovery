package uk.bl.wa.hadoop.indexer.mdx;

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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.indexer.WARCIndexerOptions;
import uk.bl.wa.hadoop.indexer.WARCIndexerRunner;
import uk.bl.wa.hadoop.mapreduce.mdx.MDXReduplicatingReducer;
import uk.bl.wa.util.ConfigPrinter;

/**
 * WARCIndexerRunner
 * 
 * Extracts text/metadata using from a series of Archive files.
 * 
 * @author rcoram
 */

@SuppressWarnings({ "deprecation" })
public class WARCMDXGenerator extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WARCMDXGenerator.class);
    private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-c <config file>] [-d] [Dump config.] [-w] [Wait for completion.]";
    private static final String CLI_HEADER = "MapReduce method for extracting metadata/text from Archive Records";
    public static final String CONFIG_PROPERTIES = "warc_indexer_config";

    WARCIndexerOptions opts = new WARCIndexerOptions();

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
            throws IOException,
            KeeperException,
            InterruptedException {

        // Store application properties where the mappers/reducers can access
        // them
        Config index_conf;
        if (opts.config != null) {
            LOG.info("Loading config from: " + opts.config);
            index_conf = ConfigFactory.parseFile(opts.config);
        } else {
            LOG.info("Using default config: mdx");
            index_conf = ConfigFactory.load("mdx");
        }
        if (opts.dump) {
            ConfigPrinter.print(index_conf);
            System.exit(0);
        }
        conf.set(CONFIG_PROPERTIES, index_conf.withOnlyPath("warc").root()
                .render(ConfigRenderOptions.concise()));
        LOG.info("Loaded warc config: " + index_conf.getString("warc.title"));

        // Add input paths:
        LOG.info("Reading input files...");
        String line = null;
        BufferedReader br = new BufferedReader(new FileReader(opts.input));
        while ((line = br.readLine()) != null) {
            FileInputFormat.addInputPath(conf, new Path(line));
        }
        br.close();
        LOG.info("Read " + FileInputFormat.getInputPaths(conf).length
                + " input files.");

        FileOutputFormat.setOutputPath(conf, new Path(opts.output));

        conf.setJobName(opts.input + "_" + System.currentTimeMillis());
        conf.setInputFormat(ArchiveFileInputFormat.class);
        conf.setMapperClass(WARCMDXMapper.class);
        conf.setReducerClass(MDXReduplicatingReducer.class);
        // Sequence file output?
        // conf.setOutputFormat(SequenceFileOutputFormat.class);
        // SequenceFileOutputFormat.setOutputCompressionType(conf,
        // CompressionType.BLOCK);
        // OR TextOutputFormat?
        conf.setOutputFormat(TextOutputFormat.class);
        // conf.set("map.output.key.field.separator", "");
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
        conf.setNumReduceTasks(opts.num_reducers);
    }

    /**
     * 
     * Run the job:
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * 
     */
    public int run(String[] args)
            throws IOException,
            KeeperException, InterruptedException {

        CommandLine cli = new CommandLine(opts);
        // SolrOptions solrOpts = new SolrWebServer.SolrOptions();
        // cli.addMixin("solrOpts", solrOpts);
        ParseResult pr = cli.parseArgs(args);
        if (pr.isUsageHelpRequested()) {
            cli.usage(System.out);
            return 0;
        }

        // Set up the base conf:
        JobConf conf = new JobConf(getConf(), WARCMDXGenerator.class);

        // Store the args in there, so Mapper/Reducer can read them:
        conf.set("commandline.args", String.join("@@@", args));

        // Get the job configuration:
        this.createJobConf(conf, args);

        // Submit it:
        if (opts.wait) {
            JobClient.runJob(conf);
        } else {
            JobClient client = new JobClient(conf);
            client.submitJob(conf);
        }
        return 0;
    }

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WARCMDXGenerator(), args);
        System.exit(ret);
    }

}
