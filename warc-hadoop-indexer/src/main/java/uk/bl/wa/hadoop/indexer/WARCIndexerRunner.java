package uk.bl.wa.hadoop.indexer;

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
import java.time.Instant;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.solr.SolrWebServer.SolrOptions;
import uk.bl.wa.util.ConfigPrinter;

/**
 * WARCIndexerRunner
 * 
 * Extracts text/metadata using from a series of Archive files.
 * 
 * @author rcoram
 */

@SuppressWarnings({ "deprecation" })
public class WARCIndexerRunner extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(WARCIndexerRunner.class);
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
    protected void createJobConf(
            JobConf conf)
            throws IOException,
            KeeperException,
            InterruptedException {

        // Store application properties where the mappers/reducers can access
        // them
        Config index_conf;
        if (opts.config != null) {
            index_conf = ConfigFactory.parseFile(opts.config);
        } else {
            index_conf = ConfigFactory.load();
        }
        if (opts.dump) {
            ConfigPrinter.print(index_conf);
            System.exit(0);
        }
        // Store the properties:
        conf.set(CONFIG_PROPERTIES, index_conf.withOnlyPath("warc").root()
                .render(ConfigRenderOptions.concise()));
        LOG.info("Loaded warc config.");
        LOG.info(index_conf.getString("warc.title"));
        if (index_conf.getBoolean("warc.solr.use_hash_url_id")) {
            LOG.info("Using hash-based ID.");
        }
        if (index_conf.hasPath("warc.solr.zookeepers")) {
            LOG.info("Using Zookeepers.");
        } else {
            LOG.info("Using SolrServers.");
        }

        // Also set reduce speculative execution off, avoiding duplicate
        // submissions to Solr.
        conf.set("mapred.reduce.tasks.speculative.execution", "false");

        // Reducer count dependent on concurrent HTTP connections to Solr
        // server.
        int numReducers = opts.num_reducers;

        // Add input paths:
        LOG.info("Reading list of input files from " + opts.input + "...");
        String line = null;
        BufferedReader br = new BufferedReader(new FileReader(opts.input));
        while ((line = br.readLine()) != null) {
            FileInputFormat.addInputPath(conf, new Path(line));
        }
        br.close();
        LOG.info("Read " + FileInputFormat.getInputPaths(conf).length
                + " input files.");

        FileOutputFormat.setOutputPath(conf, new Path(opts.output));

        conf.setJobName(
                "WARCIndexer " + opts.input + " @ " + Instant.now().toString());
        conf.setInputFormat(ArchiveFileInputFormat.class);
        conf.setMapperClass(WARCIndexerMapper.class);
        conf.setReducerClass(WARCIndexerReducer.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Compress the output from the maps, to cut down temp space
        // requirements between map and reduce.
        conf.setBoolean("mapreduce.map.output.compress", true); // Wrong syntax
        // for 0.20.x ?
        conf.set("mapred.compress.map.output", "true");
        // conf.set("mapred.map.output.compression.codec",
        // "org.apache.hadoop.io.compress.GzipCodec");
        // Ensure the JARs we provide take precedence over ones from Hadoop:
        conf.setBoolean("mapreduce.task.classpath.user.precedence", true);

        conf.setBoolean("mapred.output.oai-pmh", opts.xml);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(WritableSolrRecord.class);
        conf.setNumReduceTasks(numReducers);
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
        // Process remaining args list this:
        CommandLine cli = new CommandLine(opts);
        // SolrOptions solrOpts = new SolrWebServer.SolrOptions();
        // cli.addMixin("solrOpts", solrOpts);
        ParseResult pr = cli.parseArgs(args);
        if (pr.isUsageHelpRequested()) {
            cli.usage(System.out);
            return 0;
        }

        // Set up the base conf:
        JobConf conf = new JobConf(getConf(), WARCIndexerRunner.class);

        // Store the args in there, so Mapper/Reducer can read them:
        conf.set("commandline.args", String.join("@@@", args));

        // Get the job configuration:
        this.createJobConf(conf);

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
        int ret = ToolRunner.run(new WARCIndexerRunner(), args);
        System.exit(ret);
    }

}
