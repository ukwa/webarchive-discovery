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
import org.json.JSONArray;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.mapred.io.KeylessTextOutputFormat;
import uk.bl.wa.hadoop.mapred.FrequencyCountingReducer;
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
public class MDXSeqStatsGenerator extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(MDXSeqStatsGenerator.class);
    private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-r <#reducers>] [-w] [Wait for completion.]";
    private static final String CLI_HEADER = "MapReduce job extracting data from MDX Sequence Files.";

    private String inputPath;
    private String outputPath;
    private boolean wait;

    public static String FORMATS_SUMMARY_NAME = "formats";
    public static String FORMATS_FFB_NAME = "formatsExt";
    public static String HOST_LINKS_NAME = "hostLinks";
    public static String GEO_SUMMARY_NAME = "geo";
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
        conf.setMapperClass(MDXSeqStatsMapper.class);
        conf.setReducerClass(FrequencyCountingReducer.class);
        conf.setOutputFormat(KeylessTextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setNumReduceTasks(numReducers);
        
        MultipleOutputs.addMultiNamedOutput(conf, FORMATS_SUMMARY_NAME,
                KeylessTextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, FORMATS_FFB_NAME,
                KeylessTextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, HOST_LINKS_NAME,
                KeylessTextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addMultiNamedOutput(conf, GEO_SUMMARY_NAME,
                KeylessTextOutputFormat.class, Text.class, Text.class);

        KeylessTextOutputFormat.setCompressOutput(conf, true);
        KeylessTextOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
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

        @Override
        public void map(Text key, Text value,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            try {
            // Parse the MDX:
            MDX mdx = new MDX(value.toString());

                if (mdx.getTs() == null) {
                    // FIXME Hack for older-style records, which used 'ts'
                    // instead of
                    // MDX.TIMESTAMP:
                    mdx.setTs(mdx.getString("ts"));
                }

            String year = mdx.getTs().substring(0, 4);

            // Only extract from requests:
            if (!"request".equals(mdx.getRecordType())) {
                // Generate format summary:
                if (scanFormats) {
                        String cts = mdx.getJSONObject("properties")
                                .optString(SolrFields.CONTENT_TYPE_SERVED);
                        String ctt = mdx.getJSONObject("properties")
                                .optString(SolrFields.CONTENT_TYPE_TIKA);
                        String ctd = mdx.getJSONObject("properties")
                                .optString(SolrFields.CONTENT_TYPE_DROID);
                    output.collect(new Text(FORMATS_SUMMARY_NAME + KEY_PREFIX
                            + year), new Text(year + "\t" + cts + "\t" + ctt
                            + "\t" + ctd));

                        String ct = mdx.getJSONObject("properties")
                                .optString(SolrFields.SOLR_CONTENT_TYPE);
                        String ctext = mdx.getJSONObject("properties")
                                .optString(SolrFields.CONTENT_TYPE_EXT);
                        String ctffb = mdx.getJSONObject("properties")
                                .optString(SolrFields.CONTENT_FFB);
                    output.collect(new Text(FORMATS_FFB_NAME + KEY_PREFIX
                            + year), new Text(year + "\t" + ct + "\t" + ctext
                            + "\t" + ctffb));
                }
                // Generate host link graph
                if (scanHostLinks) {
                        String host = mdx.getJSONObject("properties")
                                .optString(SolrFields.SOLR_HOST);
                    JSONArray hosts = mdx
                                .getJSONObject("properties")
                                .optJSONArray(SolrFields.SOLR_LINKS_HOSTS);
                    if (hosts != null) {
                        for (int i = 0; i < hosts.length(); i++) {
                            String link_host = hosts.getString(i);
                            // Record the link:
                            String link = host + "\t" + link_host;
                            // Make a sub-key for the reducer so individual
                            // reducers don't get overloaded:
                            String host_key = host;
                            if (host != null && host.length() > 3)
                                host_key = host_key.substring(0, 3);
                            // And collect:
                            output.collect(
                                    new Text(HOST_LINKS_NAME + KEY_PREFIX
                                            + year + KEY_PREFIX + host_key),
                                    new Text(year + "\t" + link));
                        }
                        // Reporter;
                        reporter.incrCounter("MDX-Records", "Hosts", 1);
                    } else {
                        // Reporter that hosts was null;
                        reporter.incrCounter("MDX-Records", "Hosts-Null", 1);
                    }

                }
                // Now look for postcodes and locations:
                    JSONArray postcodes = mdx.getJSONObject("properties")
                            .optJSONArray(SolrFields.POSTCODE);
                    JSONArray locations = mdx.getJSONObject("properties")
                            .optJSONArray(SolrFields.LOCATIONS);
                if (postcodes != null) {
                    for (int i = 0; i < postcodes.length(); i++) {
                        String location = "";
                        if (locations != null
                                && locations.length() == postcodes.length()) {
                            location = locations.getString(i);
                        } else {
                            // Reporter;
                            reporter.incrCounter("MDX-Records",
                                    "Unresolved-Locations", 1);
                        }
                        // Full geo-index
                        // This does not work as should not go through
                        // FrequencyCountingReducer.
                        // String result = mdx.getTs() + "/" + mdx.getUrl() +
                        // "\t"
                        // + postcodes.get(i) + "\t" + location;
                        // output.collect(new Text(GEO_NAME + KEY_PREFIX
                        // + year_month),
                        // new Text(result));
                        // Geo-summary:
                        if (!"".equals(location)) {
                            String summary = year + "\t"
                                    + locations.getString(i);
                            output.collect(new Text(GEO_SUMMARY_NAME
                                    + KEY_PREFIX + year), new Text(summary));
                        } else {
                            // Reporter;
                            reporter.incrCounter("MDX-Records",
                                    "Empty-Locations", 1);
                        }
                    }
                }
            } else {
                // Reporter;
                reporter.incrCounter("MDX-Records",
                        "Ignored-" + mdx.getRecordType() + "-Record", 1);
            }
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

}
