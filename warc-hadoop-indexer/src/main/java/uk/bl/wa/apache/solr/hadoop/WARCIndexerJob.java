package uk.bl.wa.apache.solr.hadoop;

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
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.solr.hadoop;
//
//import java.io.BufferedInputStream;
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.OutputStreamWriter;
//import java.io.Writer;
//import java.text.NumberFormat;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Locale;
//import java.util.Map;
//
//import net.sourceforge.argparse4j.ArgumentParsers;
//import net.sourceforge.argparse4j.impl.Arguments;
//import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
//import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
//import net.sourceforge.argparse4j.impl.type.FileArgumentType;
//import net.sourceforge.argparse4j.inf.Argument;
//import net.sourceforge.argparse4j.inf.ArgumentGroup;
//import net.sourceforge.argparse4j.inf.ArgumentParser;
//import net.sourceforge.argparse4j.inf.ArgumentParserException;
//import net.sourceforge.argparse4j.inf.FeatureControl;
//import net.sourceforge.argparse4j.inf.Namespace;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.PathFilter;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.PropertyConfigurator;
//import org.apache.solr.common.cloud.SolrZkClient;
//import org.apache.solr.hadoop.dedup.NoChangeUpdateConflictResolver;
//import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
//import org.kitesdk.morphline.base.Fields;
//import org.restlet.engine.util.AlphaNumericComparator;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import uk.bl.wa.hadoop.indexer.WARCIndexerRunner;
//import uk.bl.wa.util.ConfigPrinter;
//
//import com.google.common.base.Charsets;
//import com.google.common.base.Preconditions;
//import com.google.common.io.ByteStreams;
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigFactory;
//import com.typesafe.config.ConfigRenderOptions;
//
//
///**
// * This is based on MapReduceIndexerTool and should be more powerful than
// * hacking it together manually, but depends on Hadoop 2.x.x series code so has
// * problems running against our aging cluster.
// * 
// * Public API for a MapReduce batch job driver that creates a set of Solr index
// * shards from a set of input files and writes the indexes into HDFS, in a
// * flexible, scalable and fault-tolerant manner. Also supports merging the
// * output shards into a set of live customer facing Solr servers, typically a
// * SolrCloud.
// */
//public class WARCIndexerJob extends Configured implements Tool {
//
//    Job job; // visible for testing only
//
//    public static final String RESULTS_DIR = "results";
//
//    static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD = WARCIndexerJob.class
//            .getName() + ".mainMemoryRandomizationThreshold";
//
//    private static final String FULL_INPUT_LIST = "full-input-list.txt";
//
//    private static final Logger LOG = LoggerFactory
//            .getLogger(WARCIndexerJob.class);
//
//
//    /**
//     * See http://argparse4j.sourceforge.net and for details see
//     * http://argparse4j.sourceforge.net/usage.html
//     */
//    static final class MyArgumentParser {
//
//        private static final String SHOW_NON_SOLR_CLOUD = "--show-non-solr-cloud";
//
//        private boolean showNonSolrCloud = false;
//
//        /**
//         * Parses the given command line arguments.
//         * 
//         * @return exitCode null indicates the caller shall proceed with
//         *         processing, non-null indicates the caller shall exit the
//         *         program with the given exit status code.
//         */
//        public Integer parseArgs(String[] args, Configuration conf, Options opts) {
//            assert args != null;
//            assert conf != null;
//            assert opts != null;
//
//            if (args.length == 0) {
//                args = new String[] { "--help" };
//            }
//
//            showNonSolrCloud = Arrays.asList(args)
//                    .contains(SHOW_NON_SOLR_CLOUD); // intercept it first
//
//            ArgumentParser parser = ArgumentParsers
//                    .newArgumentParser(
//                            "hadoop [GenericOptions]... jar solr-map-reduce-*.jar ",
//                            false)
//                    .defaultHelp(true)
//                    .description(
//                            "MapReduce batch job driver that takes a morphline and creates a set of Solr index shards from a set of input files "
//                                    + "and writes the indexes into HDFS, in a flexible, scalable and fault-tolerant manner. "
//                                    + "It also supports merging the output shards into a set of live customer facing Solr servers, "
//                                    + "typically a SolrCloud. The program proceeds in several consecutive MapReduce based phases, as follows:"
//                                    + "\n\n"
//                                    + "1) Randomization phase: This (parallel) phase randomizes the list of input files in order to spread "
//                                    + "indexing load more evenly among the mappers of the subsequent phase."
//                                    + "\n\n"
//                                    + "2) Mapper phase: This (parallel) phase takes the input files, extracts the relevant content, transforms it "
//                                    + "and hands SolrInputDocuments to a set of reducers. "
//                                    + "The ETL functionality is flexible and "
//                                    + "customizable using chains of arbitrary morphline commands that pipe records from one transformation command to another. "
//                                    + "Commands to parse and transform a set of standard data formats such as Avro, CSV, Text, HTML, XML, "
//                                    + "PDF, Word, Excel, etc. are provided out of the box, and additional custom commands and parsers for additional "
//                                    + "file or data formats can be added as morphline plugins. "
//                                    + "This is done by implementing a simple Java interface that consumes a record (e.g. a file in the form of an InputStream "
//                                    + "plus some headers plus contextual metadata) and generates as output zero or more records. "
//                                    + "Any kind of data format can be indexed and any Solr documents for any kind of Solr schema can be generated, "
//                                    + "and any custom ETL logic can be registered and executed.\n"
//                                    + "Record fields, including MIME types, can also explicitly be passed by force from the CLI to the morphline, for example: "
//                                    + "hadoop ... -D "
//                                    + MorphlineMapRunner.MORPHLINE_FIELD_PREFIX
//                                    + Fields.ATTACHMENT_MIME_TYPE
//                                    + "=text/csv"
//                                    + "\n\n"
//                                    + "3) Reducer phase: This (parallel) phase loads the mapper's SolrInputDocuments into one EmbeddedSolrServer per reducer. "
//                                    + "Each such reducer and Solr server can be seen as a (micro) shard. The Solr servers store their "
//                                    + "data in HDFS."
//                                    + "\n\n"
//                                    + "4) Mapper-only merge phase: This (parallel) phase merges the set of reducer shards into the number of solr "
//                                    + "shards expected by the user, using a mapper-only job. This phase is omitted if the number "
//                                    + "of shards is already equal to the number of shards expected by the user. "
//                                    + "\n\n"
//                                    + "5) Go-live phase: This optional (parallel) phase merges the output shards of the previous phase into a set of "
//                                    + "live customer facing Solr servers, typically a SolrCloud. "
//                                    + "If this phase is omitted you can explicitly point each Solr server to one of the HDFS output shard directories."
//                                    + "\n\n"
//                                    + "Fault Tolerance: Mapper and reducer task attempts are retried on failure per the standard MapReduce semantics. "
//                                    + "On program startup all data in the --output-dir is deleted if that output directory already exists. "
//                                    + "If the whole job fails you can retry simply by rerunning the program again using the same arguments.");
//
//            parser.addArgument("--help", "-help", "-h")
//                    .help("Show this help message and exit")
//                    .action(new HelpArgumentAction() {
//                        @Override
//                        public void run(ArgumentParser parser, Argument arg,
//                                Map<String, Object> attrs, String flag,
//                                Object value) throws ArgumentParserException {
//                            parser.printHelp();
//                            System.out.println();
//                            System.out.print(ToolRunnerHelpFormatter
//                                    .getGenericCommandUsage());
//                            // ToolRunner.printGenericCommandUsage(System.out);
//                            System.out
//                                    .println("Examples: \n\n"
//                                            +
//
//              "# (Re)index an Avro based Twitter tweet file:\n" +
//              "sudo -u hdfs hadoop \\\n" + 
//              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" +
//              "  jar target/solr-map-reduce-*.jar \\\n" +
//              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//                                            // "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n"
//                                            // +
//              "  --log4j src/test/resources/log4j.properties \\\n" + 
//              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n" + 
//              "  --solr-home-dir src/test/resources/solr/minimr \\\n" +
//              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
//              "  --shards 1 \\\n" + 
//              "  hdfs:///user/$USER/test-documents/sample-statuses-20120906-141433.avro\n" +
//              "\n" +
//              "# Go live by merging resulting index shards into a live Solr cluster\n" +
//              "# (explicitly specify Solr URLs - for a SolrCloud cluster see next example):\n" +
//              "sudo -u hdfs hadoop \\\n" + 
//              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" +
//              "  jar target/solr-map-reduce-*.jar \\\n" +
//              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//                                            // "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n"
//                                            // +
//              "  --log4j src/test/resources/log4j.properties \\\n" + 
//              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n" + 
//              "  --solr-home-dir src/test/resources/solr/minimr \\\n" + 
//              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
//              "  --shard-url http://solr001.mycompany.com:8983/solr/collection1 \\\n" + 
//              "  --shard-url http://solr002.mycompany.com:8983/solr/collection1 \\\n" + 
//              "  --go-live \\\n" + 
//              "  hdfs:///user/foo/indir\n" +  
//              "\n" +
//              "# Go live by merging resulting index shards into a live SolrCloud cluster\n" +
//              "# (discover shards and Solr URLs through ZooKeeper):\n" +
//              "sudo -u hdfs hadoop \\\n" + 
//              "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n" +
//              "  jar target/solr-map-reduce-*.jar \\\n" +
//              "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
//                                            // "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n"
//                                            // +
//              "  --log4j src/test/resources/log4j.properties \\\n" + 
//              "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n" + 
//              "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
//              "  --zk-host zk01.mycompany.com:2181/solr \\\n" + 
//              "  --collection collection1 \\\n" + 
//              "  --go-live \\\n" + 
//              "  hdfs:///user/foo/indir\n"
//);
//                            throw new FoundHelpArgument(); // Trick to prevent
//                                                            // processing of any
//                                                            // remaining
//                                                            // arguments
//                        }
//                    });
//
//            ArgumentGroup requiredGroup = parser
//                    .addArgumentGroup("Required arguments");
//
//            Argument outputDirArg = requiredGroup
//                    .addArgument("--output-dir")
//                    .metavar("HDFS_URI")
//                    .type(new PathArgumentType(conf) {
//                        @Override
//                        public Path convert(ArgumentParser parser,
//                                Argument arg, String value)
//                                throws ArgumentParserException {
//                            Path path = super.convert(parser, arg, value);
//                            if ("hdfs".equals(path.toUri().getScheme())
//                                    && path.toUri().getAuthority() == null) {
//                                // TODO: consider defaulting to hadoop's
//                                // fs.default.name here or in
//                                // SolrRecordWriter.createEmbeddedSolrServer()
//                                throw new ArgumentParserException(
//                                        "Missing authority in path URI: "
//                                                + path, parser);
//                            }
//                            return path;
//                        }
//                    }.verifyHasScheme().verifyIsAbsolute()
//                            .verifyCanWriteParent())
//                    .required(true)
//                    .help("HDFS directory to write Solr indexes to. Inside there one output directory per shard will be generated. "
//                            + "Example: hdfs://c2202.mycompany.com/user/$USER/test");
//
//            Argument inputListArg = parser
//                    .addArgument("--input-list")
//                    .action(Arguments.append())
//                    .metavar("URI")
//                    // .type(new
//                    // PathArgumentType(fs).verifyExists().verifyCanRead())
//                    .type(Path.class)
//                    .help("Local URI or HDFS URI of a UTF-8 encoded file containing a list of HDFS URIs to index, "
//                            + "one URI per line in the file. If '-' is specified, URIs are read from the standard input. "
//                            + "Multiple --input-list arguments can be specified.");
//
//            Argument solrHomeDirArg = nonSolrCloud(parser
//                    .addArgument("--solr-home-dir")
//                    .metavar("DIR")
//                    .type(new FileArgumentType() {
//                        @Override
//                        public File convert(ArgumentParser parser,
//                                Argument arg, String value)
//                                throws ArgumentParserException {
//                            File solrHomeDir = super
//                                    .convert(parser, arg, value);
//                            File solrConfigFile = new File(new File(
//                                    solrHomeDir, "conf"), "solrconfig.xml");
//                            new FileArgumentType()
//                                    .verifyExists()
//                                    .verifyIsFile()
//                                    .verifyCanRead()
//                                    .convert(parser, arg,
//                                            solrConfigFile.getPath());
//                            return solrHomeDir;
//                        }
//                    }.verifyIsDirectory().verifyCanRead())
//                    .required(false)
//                    .help("Relative or absolute path to a local dir containing Solr conf/ dir and in particular "
//                            + "conf/solrconfig.xml and optionally also lib/ dir. This directory will be uploaded to each MR task. "
//                            + "Example: src/test/resources/solr/minimr"));
//
//            Argument updateConflictResolverArg = parser
//                    .addArgument("--update-conflict-resolver")
//                    .metavar("FQCN")
//                    .type(String.class)
//                    .setDefault(NoChangeUpdateConflictResolver.class.getName())
//                    .help("Fully qualified class name of a Java class that implements the UpdateConflictResolver interface. "
//                            + "This enables deduplication and ordering of a series of document updates for the same unique document "
//                            + "key. For example, a MapReduce batch job might index multiple files in the same job where some of the "
//                            + "files contain old and new versions of the very same document, using the same unique document key.\n"
//                            + "Typically, implementations of this interface forbid collisions by throwing an exception, or ignore all but "
//                            + "the most recent document version, or, in the general case, order colliding updates ascending from least "
//                            + "recent to most recent (partial) update. The caller of this interface (i.e. the Hadoop Reducer) will then "
//                            + "apply the updates to Solr in the order returned by the orderUpdates() method.\n"
//                            + "The default RetainMostRecentUpdateConflictResolver implementation ignores all but the most recent document "
//                            + "version, based on a configurable numeric Solr field, which defaults to the file_last_modified timestamp");
//
//            Argument mappersArg = parser
//                    .addArgument("--mappers")
//                    .metavar("INTEGER")
//                    .type(Integer.class)
//                    .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE))
//                    // TODO: also support X% syntax where X is an integer
//                    .setDefault(-1)
//                    .help("Tuning knob that indicates the maximum number of MR mapper tasks to use. -1 indicates use all map slots "
//                            + "available on the cluster.");
//
//            Argument reducersArg = parser
//                    .addArgument("--reducers")
//                    .metavar("INTEGER")
//                    .type(Integer.class)
//                    .choices(new RangeArgumentChoice(-2, Integer.MAX_VALUE))
//                    // TODO: also support X% syntax where X is an integer
//                    .setDefault(-1)
//                    .help("Tuning knob that indicates the number of reducers to index into. "
//                            + "0 is reserved for a mapper-only feature that may ship in a future release. "
//                            + "-1 indicates use all reduce slots available on the cluster. "
//                            + "-2 indicates use one reducer per output shard, which disables the mtree merge MR algorithm. "
//                            + "The mtree merge MR algorithm improves scalability by spreading load "
//                            + "(in particular CPU load) among a number of parallel reducers that can be much larger than the number "
//                            + "of solr shards expected by the user. It can be seen as an extension of concurrent lucene merges "
//                            + "and tiered lucene merges to the clustered case. The subsequent mapper-only phase "
//                            + "merges the output of said large number of reducers to the number of shards expected by the user, "
//                            + "again by utilizing more available parallelism on the cluster.");
//
//            Argument fanoutArg = parser.addArgument("--fanout")
//                    .metavar("INTEGER").type(Integer.class)
//                    .choices(new RangeArgumentChoice(2, Integer.MAX_VALUE))
//                    .setDefault(Integer.MAX_VALUE)
//                    .help(FeatureControl.SUPPRESS);
//
//            Argument maxSegmentsArg = parser
//                    .addArgument("--max-segments")
//                    .metavar("INTEGER")
//                    .type(Integer.class)
//                    .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
//                    .setDefault(1)
//                    .help("Tuning knob that indicates the maximum number of segments to be contained on output in the index of "
//                            + "each reducer shard. After a reducer has built its output index it applies a merge policy to merge segments "
//                            + "until there are <= maxSegments lucene segments left in this index. "
//                            + "Merging segments involves reading and rewriting all data in all these segment files, "
//                            + "potentially multiple times, which is very I/O intensive and time consuming. "
//                            + "However, an index with fewer segments can later be merged faster, "
//                            + "and it can later be queried faster once deployed to a live Solr serving shard. "
//                            + "Set maxSegments to 1 to optimize the index for low query latency. "
//                            + "In a nutshell, a small maxSegments value trades indexing latency for subsequently improved query latency. "
//                            + "This can be a reasonable trade-off for batch indexing systems.");
//
//            Argument fairSchedulerPoolArg = parser
//                    .addArgument("--fair-scheduler-pool")
//                    .metavar("STRING")
//                    .help("Optional tuning knob that indicates the name of the fair scheduler pool to submit jobs to. "
//                            + "The Fair Scheduler is a pluggable MapReduce scheduler that provides a way to share large clusters. "
//                            + "Fair scheduling is a method of assigning resources to jobs such that all jobs get, on average, an "
//                            + "equal share of resources over time. When there is a single job running, that job uses the entire "
//                            + "cluster. When other jobs are submitted, tasks slots that free up are assigned to the new jobs, so "
//                            + "that each job gets roughly the same amount of CPU time. Unlike the default Hadoop scheduler, which "
//                            + "forms a queue of jobs, this lets short jobs finish in reasonable time while not starving long jobs. "
//                            + "It is also an easy way to share a cluster between multiple of users. Fair sharing can also work with "
//                            + "job priorities - the priorities are used as weights to determine the fraction of total compute time "
//                            + "that each job gets.");
//
//            Argument dryRunArg = parser
//                    .addArgument("--dry-run")
//                    .action(Arguments.storeTrue())
//                    .help("Run in local mode and print documents to stdout instead of loading them into Solr. This executes "
//                            + "the morphline in the client process (without submitting a job to MR) for quicker turnaround during "
//                            + "early trial & debug sessions.");
//
//            Argument log4jConfigFileArg = parser
//                    .addArgument("--log4j")
//                    .metavar("FILE")
//                    .type(new FileArgumentType().verifyExists().verifyIsFile()
//                            .verifyCanRead())
//                    .help("Relative or absolute path to a log4j.properties config file on the local file system. This file "
//                            + "will be uploaded to each MR task. Example: /path/to/log4j.properties");
//
//            Argument verboseArg = parser.addArgument("--verbose", "-v")
//                    .action(Arguments.storeTrue())
//                    .help("Turn on verbose output.");
//
//            parser.addArgument(SHOW_NON_SOLR_CLOUD)
//                    .action(Arguments.storeTrue())
//                    .help("Also show options for Non-SolrCloud mode as part of --help.");
//
//            ArgumentGroup clusterInfoGroup = parser
//                    .addArgumentGroup("Cluster arguments")
//                    .description(
//                            "Arguments that provide information about your Solr cluster. "
//                                    + nonSolrCloud("If you are building shards for a SolrCloud cluster, pass the --zk-host argument. "
//                                            + "If you are building shards for "
//                                            + "a Non-SolrCloud cluster, pass the --shard-url argument one or more times. To build indexes for "
//                                            + "a replicated Non-SolrCloud cluster with --shard-url, pass replica urls consecutively and also pass --shards. "
//                                            + "Using --go-live requires either --zk-host or --shard-url."));
//
//            Argument zkHostArg = clusterInfoGroup
//                    .addArgument("--zk-host")
//                    .metavar("STRING")
//                    .type(String.class)
//                    .help("The address of a ZooKeeper ensemble being used by a SolrCloud cluster. "
//                            + "This ZooKeeper ensemble will be examined to determine the number of output "
//                            + "shards to create as well as the Solr URLs to merge the output shards into when using the --go-live option. "
//                            + "Requires that you also pass the --collection to merge the shards into.\n"
//                            + "\n"
//                            + "The --zk-host option implements the same partitioning semantics as the standard SolrCloud "
//                            + "Near-Real-Time (NRT) API. This enables to mix batch updates from MapReduce ingestion with "
//                            + "updates from standard Solr NRT ingestion on the same SolrCloud cluster, "
//                            + "using identical unique document keys.\n"
//                            + "\n"
//                            + "Format is: a list of comma separated host:port pairs, each corresponding to a zk "
//                            + "server. Example: '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183' If "
//                            + "the optional chroot suffix is used the example would look "
//                            + "like: '127.0.0.1:2181/solr,127.0.0.1:2182/solr,127.0.0.1:2183/solr' "
//                            + "where the client would be rooted at '/solr' and all paths "
//                            + "would be relative to this root - i.e. getting/setting/etc... "
//                            + "'/foo/bar' would result in operations being run on "
//                            + "'/solr/foo/bar' (from the server perspective).\n"
//                            + nonSolrCloud("\n"
//                                    + "If --solr-home-dir is not specified, the Solr home directory for the collection "
//                                    + "will be downloaded from this ZooKeeper ensemble."));
//
//            Argument shardsArg = nonSolrCloud(clusterInfoGroup
//                    .addArgument("--shards").metavar("INTEGER")
//                    .type(Integer.class)
//                    .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
//                    .help("Number of output shards to generate."));
//
//            Argument collectionArg = parser
//                    .addArgument("--collection")
//                    .metavar("STRING")
//                    .help("The SolrCloud collection to merge shards into when using --zk-host. Example: collection1");
//
//            // trailing positional arguments
//            Argument inputFilesArg = parser
//                    .addArgument("input-files")
//                    .metavar("HDFS_URI")
//                    .type(new PathArgumentType(conf).verifyHasScheme()
//                            .verifyExists().verifyCanRead()).nargs("*")
//                    .setDefault()
//                    .help("HDFS URI of file or directory tree to index.");
//
//            Argument configPathArg = parser.addArgument("--config-path")
//                    .metavar("STRING").help("The indexer config file to load.");
//
//            Argument dumpConfigArg = parser.addArgument("--dump-config")
//                    .action(Arguments.storeTrue()).help("Dump indexer config.");
//
//            Namespace ns;
//            try {
//                ns = parser.parseArgs(args);
//            } catch (FoundHelpArgument e) {
//                return 0;
//            } catch (ArgumentParserException e) {
//                parser.handleError(e);
//                return 1;
//            }
//
//            opts.log4jConfigFile = (File) ns.get(log4jConfigFileArg.getDest());
//            if (opts.log4jConfigFile != null) {
//                PropertyConfigurator.configure(opts.log4jConfigFile.getPath());
//            }
//            LOG.debug("Parsed command line args: {}", ns);
//
//            opts.inputLists = ns.getList(inputListArg.getDest());
//            if (opts.inputLists == null) {
//                opts.inputLists = Collections.EMPTY_LIST;
//            }
//            opts.inputFiles = ns.getList(inputFilesArg.getDest());
//            opts.outputDir = (Path) ns.get(outputDirArg.getDest());
//            opts.mappers = ns.getInt(mappersArg.getDest());
//            opts.reducers = ns.getInt(reducersArg.getDest());
//            opts.updateConflictResolver = ns
//                    .getString(updateConflictResolverArg.getDest());
//            opts.fanout = ns.getInt(fanoutArg.getDest());
//            opts.maxSegments = ns.getInt(maxSegmentsArg.getDest());
//            opts.solrHomeDir = (File) ns.get(solrHomeDirArg.getDest());
//            opts.fairSchedulerPool = ns.getString(fairSchedulerPoolArg
//                    .getDest());
//            opts.isDryRun = ns.getBoolean(dryRunArg.getDest());
//            opts.isVerbose = ns.getBoolean(verboseArg.getDest());
//            opts.zkHost = ns.getString(zkHostArg.getDest());
//            opts.shards = ns.getInt(shardsArg.getDest());
//            opts.collection = ns.getString(collectionArg.getDest());
//
//            opts.configPath = ns.getString(configPathArg.getDest());
//            opts.dumpConfig = ns.getBoolean(dumpConfigArg.getDest());
//
//            try {
//                if (opts.reducers == 0) {
//                    throw new ArgumentParserException(
//                            "--reducers must not be zero", parser);
//                }
//            } catch (ArgumentParserException e) {
//                parser.handleError(e);
//                return 1;
//            }
//
//            if (opts.inputLists.isEmpty() && opts.inputFiles.isEmpty()) {
//                LOG.info("No input files specified - nothing to process");
//                return 0; // nothing to process
//            }
//            return null;
//        }
//
//        // make it a "hidden" option, i.e. the option is functional and enabled
//        // but not shown in --help output
//        private Argument nonSolrCloud(Argument arg) {
//            return showNonSolrCloud ? arg : arg.help(FeatureControl.SUPPRESS);
//        }
//
//        private String nonSolrCloud(String msg) {
//            return showNonSolrCloud ? msg : "";
//        }
//
//        /**
//         * Marker trick to prevent processing of any remaining arguments once
//         * --help option has been parsed
//         */
//        private static final class FoundHelpArgument extends RuntimeException {
//        }
//    }
//
//    // END OF INNER CLASS
//
//    static List<List<String>> buildShardUrls(List<Object> urls,
//            Integer numShards) {
//        if (urls == null)
//            return null;
//        List<List<String>> shardUrls = new ArrayList<List<String>>(urls.size());
//        List<String> list = null;
//
//        int sz;
//        if (numShards == null) {
//            numShards = urls.size();
//        }
//        sz = (int) Math.ceil(urls.size() / (float) numShards);
//        for (int i = 0; i < urls.size(); i++) {
//            if (i % sz == 0) {
//                list = new ArrayList<String>();
//                shardUrls.add(list);
//            }
//            list.add((String) urls.get(i));
//        }
//
//        return shardUrls;
//    }
//
//    static final class Options {
//        String collection;
//        String zkHost;
//        List<Path> inputLists;
//        List<Path> inputFiles;
//        Path outputDir;
//        int mappers;
//        int reducers;
//        String updateConflictResolver;
//        int fanout;
//        Integer shards;
//        int maxSegments;
//        File solrHomeDir;
//        String fairSchedulerPool;
//        boolean isDryRun;
//        File log4jConfigFile;
//        boolean isVerbose;
//        String configPath;
//        boolean dumpConfig;
//    }
//
//    // END OF INNER CLASS
//
//
//    /** API for command line clients */
//    public static void main(String[] args) throws Exception {
//        int res = ToolRunner.run(new Configuration(),
// new WARCIndexerJob(),
//                args);
//        System.exit(res);
//    }
//
//    public WARCIndexerJob() {
//    }
//
//    @Override
//    public int run(String[] args) throws Exception {
//        Options opts = new Options();
//        Integer exitCode = new MyArgumentParser().parseArgs(args, getConf(),
//                opts);
//        if (exitCode != null) {
//            return exitCode;
//        }
//        return run(opts);
//    }
//
//    /**
//     * API for Java clients; visible for testing; may become a public API
//     * eventually
//     */
//    int run(Options options) throws Exception {
//        if (getConf().getBoolean("isMR1", false)
//                && "local".equals(getConf().get("mapred.job.tracker"))) {
//            throw new IllegalStateException(
//                    "Running with LocalJobRunner (i.e. all of Hadoop inside a single JVM) is not supported "
//                            + "because LocalJobRunner does not (yet) implement the Hadoop Distributed Cache feature, "
//                            + "which is required for passing files via --files and --libjars");
//        }
//
//        long programStartTime = System.nanoTime();
//        if (options.fairSchedulerPool != null) {
//            getConf().set("mapred.fairscheduler.pool",
//                    options.fairSchedulerPool);
//        }
//        getConf().setInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS,
//                options.maxSegments);
//
//        // switch off a false warning about allegedly not implementing Tool
//        // also see
//        // http://hadoop.6.n7.nabble.com/GenericOptionsParser-warning-td8103.html
//        // also see https://issues.apache.org/jira/browse/HADOOP-8183
//        getConf().setBoolean("mapred.used.genericoptionsparser", true);
//
//        if (options.log4jConfigFile != null) {
//            Utils.setLogConfigFile(options.log4jConfigFile, getConf());
//            addDistributedCacheFile(options.log4jConfigFile, getConf());
//        }
//
//        job = Job.getInstance(getConf());
//        job.setJarByClass(getClass());
//
//        int mappers = new JobClient(job.getConfiguration()).getClusterStatus()
//                .getMaxMapTasks(); // MR1
//        // int mappers =
//        // job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn
//        // only
//        LOG.info("Cluster reports {} mapper slots", mappers);
//
//        if (options.mappers == -1) {
//            mappers = 8 * mappers; // better accomodate stragglers
//        } else {
//            mappers = options.mappers;
//        }
//        if (mappers <= 0) {
//            throw new IllegalStateException("Illegal number of mappers: "
//                    + mappers);
//        }
//        options.mappers = mappers;
//
//        FileSystem fs = options.outputDir.getFileSystem(job.getConfiguration());
//        if (fs.exists(options.outputDir)
//                && !delete(options.outputDir, true, fs)) {
//            return -1;
//        }
//        Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);
//        Path outputReduceDir = new Path(options.outputDir, "reducers");
//        Path outputStep1Dir = new Path(options.outputDir, "tmp1");
//        Path outputStep2Dir = new Path(options.outputDir, "tmp2");
//        Path outputTreeMergeStep = new Path(options.outputDir,
//                "mtree-merge-output");
//        Path fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);
//
//        LOG.debug("Creating list of input files for mappers: {}", fullInputList);
//        long numFiles = addInputFiles(options.inputFiles, options.inputLists,
//                fullInputList, job.getConfiguration());
//        if (numFiles == 0) {
//            LOG.info("No input files found - nothing to process");
//            return 0;
//        }
//        int numLinesPerSplit = (int) ceilDivide(numFiles, mappers);
//        if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to
//                                    // int?
//            numLinesPerSplit = Integer.MAX_VALUE;
//        }
//        numLinesPerSplit = Math.max(1, numLinesPerSplit);
//
//        int realMappers = Math.min(mappers,
//                (int) ceilDivide(numFiles, numLinesPerSplit));
//        calculateNumReducers(options, realMappers);
//        int reducers = options.reducers;
//        LOG.info(
//                "Using these parameters: "
//                        + "numFiles: {}, mappers: {}, realMappers: {}, reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
//                new Object[] { numFiles, mappers, realMappers, reducers,
//                        options.shards, options.fanout, options.maxSegments });
//
//
//        long startTime = System.nanoTime();
//        float secs = (System.nanoTime() - startTime) / (float) (10 ^ 9);
//
//        // ---- ---- ----
//
//        // Store application properties where the mappers/reducers can access
//        // them
//        Config index_conf;
//        if (options.configPath != null) {
//            index_conf = ConfigFactory.parseFile(new File(options.configPath));
//        } else {
//            index_conf = ConfigFactory.load();
//        }
//        if (options.dumpConfig) {
//            ConfigPrinter.print(index_conf);
//            System.exit(0);
//        }
//        job.getConfiguration().set(
//                WARCIndexerRunner.CONFIG_PROPERTIES,
//                index_conf.withOnlyPath("warc").root()
//                .render(ConfigRenderOptions.concise()));
//
//        job.setInputFormatClass(WebArchiveFileInputFormat.class);
//        Class<WebArchiveIndexerMapper> mapperClass = WebArchiveIndexerMapper.class;
//        job.setMapperClass(mapperClass);
//
//        // ---- ---- ----
//
//        FileOutputFormat.setOutputPath(job, outputReduceDir);
//        job.setJobName(getClass().getName() + "/"
//                + Utils.getShortClassName(mapperClass));
//
//        if (job.getConfiguration().get(JobContext.REDUCE_CLASS_ATTR) == null) { // enable
//                                                                                // customization
//            job.setReducerClass(SolrReducer.class);
//        }
//        if (options.updateConflictResolver == null) {
//            throw new IllegalArgumentException(
//                    "updateConflictResolver must not be null");
//        }
//        job.getConfiguration().set(SolrReducer.UPDATE_CONFLICT_RESOLVER,
//                options.updateConflictResolver);
//
//        if (options.zkHost != null) {
//            assert options.collection != null;
//            /*
//             * MapReduce partitioner that partitions the Mapper output such that
//             * each SolrInputDocument gets sent to the SolrCloud shard that it
//             * would have been sent to if the document were ingested via the
//             * standard SolrCloud Near Real Time (NRT) API.
//             * 
//             * In other words, this class implements the same partitioning
//             * semantics as the standard SolrCloud NRT API. This enables to mix
//             * batch updates from MapReduce ingestion with updates from standard
//             * NRT ingestion on the same SolrCloud cluster, using identical
//             * unique document keys.
//             */
//            if (job.getConfiguration().get(JobContext.PARTITIONER_CLASS_ATTR) == null) { // enable
//                                                                                            // customization
//                job.setPartitionerClass(SolrCloudPartitioner.class);
//            }
//            job.getConfiguration().set(SolrCloudPartitioner.ZKHOST,
//                    options.zkHost);
//            job.getConfiguration().set(SolrCloudPartitioner.COLLECTION,
//                    options.collection);
//        }
//        job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS,
//                options.shards);
//
//        job.setOutputFormatClass(SolrOutputFormat.class);
//        if (options.solrHomeDir != null) {
//            SolrOutputFormat.setupSolrHomeCache(options.solrHomeDir, job);
//        } else {
//            assert options.zkHost != null;
//            // use the config that this collection uses for the SolrHomeCache.
//            ZooKeeperInspector zki = new ZooKeeperInspector();
//            SolrZkClient zkClient = zki.getZkClient(options.zkHost);
//            try {
//                String configName = zki.readConfigName(zkClient,
//                        options.collection);
//                File tmpSolrHomeDir = zki.downloadConfigDir(zkClient,
//                        configName);
//                SolrOutputFormat.setupSolrHomeCache(tmpSolrHomeDir, job);
//                options.solrHomeDir = tmpSolrHomeDir;
//            } finally {
//                zkClient.close();
//            }
//        }
//
//        job.setNumReduceTasks(reducers);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(SolrInputDocumentWritable.class);
//        LOG.info("Indexing {} files using {} real mappers into {} reducers",
//                new Object[] { numFiles, realMappers, reducers });
//        startTime = System.nanoTime();
//        if (!waitForCompletion(job, options.isVerbose)) {
//            return -1; // job failed
//        }
//
//        secs = (System.nanoTime() - startTime) / (float) (10 ^ 9);
//        LOG.info(
//                "Done. Indexing {} files using {} real mappers into {} reducers took {} secs",
//                new Object[] { numFiles, realMappers, reducers, secs });
//
//        int mtreeMergeIterations = 0;
//        if (reducers > options.shards) {
//            mtreeMergeIterations = (int) Math.round(log(options.fanout,
//                    reducers / options.shards));
//        }
//        LOG.debug("MTree merge iterations to do: {}", mtreeMergeIterations);
//        int mtreeMergeIteration = 1;
//        while (reducers > options.shards) { // run a mtree merge iteration
//            job = Job.getInstance(getConf());
//            job.setJarByClass(getClass());
//            job.setJobName(getClass().getName() + "/"
//                    + Utils.getShortClassName(TreeMergeMapper.class));
//            job.setMapperClass(TreeMergeMapper.class);
//            job.setOutputFormatClass(TreeMergeOutputFormat.class);
//            job.setNumReduceTasks(0);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(NullWritable.class);
//            job.setInputFormatClass(NLineInputFormat.class);
//
//            Path inputStepDir = new Path(options.outputDir,
//                    "mtree-merge-input-iteration" + mtreeMergeIteration);
//            fullInputList = new Path(inputStepDir, FULL_INPUT_LIST);
//            LOG.debug(
//                    "MTree merge iteration {}/{}: Creating input list file for mappers {}",
//                    new Object[] { mtreeMergeIteration, mtreeMergeIterations,
//                            fullInputList });
//            numFiles = createTreeMergeInputDirList(outputReduceDir, fs,
//                    fullInputList);
//            if (numFiles != reducers) {
//                throw new IllegalStateException("Not same reducers: "
//                        + reducers + ", numFiles: " + numFiles);
//            }
//            NLineInputFormat.addInputPath(job, fullInputList);
//            NLineInputFormat.setNumLinesPerSplit(job, options.fanout);
//            FileOutputFormat.setOutputPath(job, outputTreeMergeStep);
//
//            LOG.info(
//                    "MTree merge iteration {}/{}: Merging {} shards into {} shards using fanout {}",
//                    new Object[] { mtreeMergeIteration, mtreeMergeIterations,
//                            reducers, (reducers / options.fanout),
//                            options.fanout });
//            startTime = System.nanoTime();
//            if (!waitForCompletion(job, options.isVerbose)) {
//                return -1; // job failed
//            }
//            if (!renameTreeMergeShardDirs(outputTreeMergeStep, job, fs)) {
//                return -1;
//            }
//            secs = (System.nanoTime() - startTime) / (float) (10 ^ 9);
//            LOG.info(
//                    "MTree merge iteration {}/{}: Done. Merging {} shards into {} shards using fanout {} took {} secs",
//                    new Object[] { mtreeMergeIteration, mtreeMergeIterations,
//                            reducers, (reducers / options.fanout),
//                            options.fanout, secs });
//
//            if (!delete(outputReduceDir, true, fs)) {
//                return -1;
//            }
//            if (!rename(outputTreeMergeStep, outputReduceDir, fs)) {
//                return -1;
//            }
//            assert reducers % options.fanout == 0;
//            reducers = reducers / options.fanout;
//            mtreeMergeIteration++;
//        }
//        assert reducers == options.shards;
//
//        // normalize output shard dir prefix, i.e.
//        // rename part-r-00000 to part-00000 (stems from zero tree merge
//        // iterations)
//        // rename part-m-00000 to part-00000 (stems from > 0 tree merge
//        // iterations)
//        for (FileStatus stats : fs.listStatus(outputReduceDir)) {
//            String dirPrefix = SolrOutputFormat.getOutputName(job);
//            Path srcPath = stats.getPath();
//            if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
//                String dstName = dirPrefix
//                        + srcPath.getName().substring(
//                                dirPrefix.length() + "-m".length());
//                Path dstPath = new Path(srcPath.getParent(), dstName);
//                if (!rename(srcPath, dstPath, fs)) {
//                    return -1;
//                }
//            }
//        }
//        ;
//
//        // publish results dir
//        if (!rename(outputReduceDir, outputResultsDir, fs)) {
//            return -1;
//        }
//
//        goodbye(job, programStartTime);
//        return 0;
//    }
//
//    private void calculateNumReducers(Options options, int realMappers)
//            throws IOException {
//        if (options.shards <= 0) {
//            throw new IllegalStateException("Illegal number of shards: "
//                    + options.shards);
//        }
//        if (options.fanout <= 1) {
//            throw new IllegalStateException("Illegal fanout: " + options.fanout);
//        }
//        if (realMappers <= 0) {
//            throw new IllegalStateException("Illegal realMappers: "
//                    + realMappers);
//        }
//
//        int reducers = new JobClient(job.getConfiguration()).getClusterStatus()
//                .getMaxReduceTasks(); // MR1
//        // reducers =
//        // job.getCluster().getClusterStatus().getReduceSlotCapacity(); // Yarn
//        // only
//        LOG.info("Cluster reports {} reduce slots", reducers);
//
//        if (options.reducers == -2) {
//            reducers = options.shards;
//        } else if (options.reducers == -1) {
//            reducers = Math.min(reducers, realMappers); // no need to use many
//                                                        // reducers when using
//                                                        // few mappers
//        } else {
//            if (options.reducers == 0) {
//                throw new IllegalStateException("Illegal zero reducers");
//            }
//            reducers = options.reducers;
//        }
//        reducers = Math.max(reducers, options.shards);
//
//        if (reducers != options.shards) {
//            // Ensure fanout isn't misconfigured. fanout can't meaningfully be
//            // larger than what would be
//            // required to merge all leaf shards in one single tree merge
//            // iteration into root shards
//            options.fanout = Math.min(options.fanout,
//                    (int) ceilDivide(reducers, options.shards));
//
//            // Ensure invariant reducers == options.shards * (fanout ^ N) where
//            // N is an integer >= 1.
//            // N is the number of mtree merge iterations.
//            // This helps to evenly spread docs among root shards and simplifies
//            // the impl of the mtree merge algorithm.
//            int s = options.shards;
//            while (s < reducers) {
//                s = s * options.fanout;
//            }
//            reducers = s;
//            assert reducers % options.fanout == 0;
//        }
//        options.reducers = reducers;
//    }
//
//    private long addInputFiles(List<Path> inputFiles, List<Path> inputLists,
//            Path fullInputList, Configuration conf) throws IOException {
//
//        long numFiles = 0;
//        FileSystem fs = fullInputList.getFileSystem(conf);
//        FSDataOutputStream out = fs.create(fullInputList);
//        try {
//            Writer writer = new BufferedWriter(new OutputStreamWriter(out,
//                    "UTF-8"));
//
//            for (Path inputFile : inputFiles) {
//                FileSystem inputFileFs = inputFile.getFileSystem(conf);
//                if (inputFileFs.exists(inputFile)) {
//                    PathFilter pathFilter = new PathFilter() {
//                        @Override
//                        public boolean accept(Path path) { // ignore "hidden"
//                                                            // files and dirs
//                            return !(path.getName().startsWith(".") || path
//                                    .getName().startsWith("_"));
//                        }
//                    };
//                    numFiles += addInputFilesRecursively(inputFile, writer,
//                            inputFileFs, pathFilter);
//                }
//            }
//
//            for (Path inputList : inputLists) {
//                InputStream in;
//                if (inputList.toString().equals("-")) {
//                    in = System.in;
//                } else if (inputList.isAbsoluteAndSchemeAuthorityNull()) {
//                    in = new BufferedInputStream(new FileInputStream(
//                            inputList.toString()));
//                } else {
//                    in = inputList.getFileSystem(conf).open(inputList);
//                }
//                try {
//                    BufferedReader reader = new BufferedReader(
//                            new InputStreamReader(in, "UTF-8"));
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        writer.write(line + "\n");
//                        numFiles++;
//                    }
//                    reader.close();
//                } finally {
//                    in.close();
//                }
//            }
//
//            writer.close();
//        } finally {
//            out.close();
//        }
//        return numFiles;
//    }
//
//    /**
//     * Add the specified file to the input set, if path is a directory then add
//     * the files contained therein.
//     */
//    private long addInputFilesRecursively(Path path, Writer writer,
//            FileSystem fs, PathFilter pathFilter) throws IOException {
//        long numFiles = 0;
//        for (FileStatus stat : fs.listStatus(path, pathFilter)) {
//            LOG.debug("Adding path {}", stat.getPath());
//            if (stat.isDirectory()) {
//                numFiles += addInputFilesRecursively(stat.getPath(), writer,
//                        fs, pathFilter);
//            } else {
//                writer.write(stat.getPath().toString() + "\n");
//                numFiles++;
//            }
//        }
//        return numFiles;
//    }
//
//    // do the same as if the user had typed 'hadoop ... --files <file>'
//    private void addDistributedCacheFile(File file, Configuration conf)
//            throws IOException {
//        String HADOOP_TMP_FILES = "tmpfiles"; // see Hadoop's
//                                                // GenericOptionsParser
//        String tmpFiles = conf.get(HADOOP_TMP_FILES, "");
//        if (tmpFiles.length() > 0) { // already present?
//            tmpFiles = tmpFiles + ",";
//        }
//        GenericOptionsParser parser = new GenericOptionsParser(
//                new Configuration(conf), new String[] { "--files",
//                        file.getCanonicalPath() });
//        String additionalTmpFiles = parser.getConfiguration().get(
//                HADOOP_TMP_FILES);
//        assert additionalTmpFiles != null;
//        assert additionalTmpFiles.length() > 0;
//        tmpFiles += additionalTmpFiles;
//        conf.set(HADOOP_TMP_FILES, tmpFiles);
//    }
//
//    private int createTreeMergeInputDirList(Path outputReduceDir,
//            FileSystem fs, Path fullInputList) throws FileNotFoundException,
//            IOException {
//
//        FileStatus[] dirs = listSortedOutputShardDirs(outputReduceDir, fs);
//        int numFiles = 0;
//        FSDataOutputStream out = fs.create(fullInputList);
//        try {
//            Writer writer = new BufferedWriter(new OutputStreamWriter(out,
//                    "UTF-8"));
//            for (FileStatus stat : dirs) {
//                LOG.debug("Adding path {}", stat.getPath());
//                Path dir = new Path(stat.getPath(), "data/index");
//                if (!fs.isDirectory(dir)) {
//                    throw new IllegalStateException("Not a directory: " + dir);
//                }
//                writer.write(dir.toString() + "\n");
//                numFiles++;
//            }
//            writer.close();
//        } finally {
//            out.close();
//        }
//        return numFiles;
//    }
//
//    private FileStatus[] listSortedOutputShardDirs(Path outputReduceDir,
//            FileSystem fs) throws FileNotFoundException, IOException {
//
//        final String dirPrefix = SolrOutputFormat.getOutputName(job);
//        FileStatus[] dirs = fs.listStatus(outputReduceDir, new PathFilter() {
//            @Override
//            public boolean accept(Path path) {
//                return path.getName().startsWith(dirPrefix);
//            }
//        });
//        for (FileStatus dir : dirs) {
//            if (!dir.isDirectory()) {
//                throw new IllegalStateException("Not a directory: "
//                        + dir.getPath());
//            }
//        }
//
//        // use alphanumeric sort (rather than lexicographical sort) to properly
//        // handle more than 99999 shards
//        Arrays.sort(dirs, new Comparator<FileStatus>() {
//            @Override
//            public int compare(FileStatus f1, FileStatus f2) {
//                return new AlphaNumericComparator().compare(f1.getPath()
//                        .getName(), f2.getPath().getName());
//            }
//        });
//
//        return dirs;
//    }
//
//    /*
//     * You can run MapReduceIndexerTool in Solrcloud mode, and once the MR job
//     * completes, you can use the standard solrj Solrcloud API to send doc
//     * updates and deletes to SolrCloud, and those updates and deletes will go
//     * to the right Solr shards, and it will work just fine.
//     * 
//     * The MapReduce framework doesn't guarantee that input split N goes to the
//     * map task with the taskId = N. The job tracker and Yarn schedule and
//     * assign tasks, considering data locality aspects, but without regard of
//     * the input split# withing the overall list of input splits. In other
//     * words, split# != taskId can be true.
//     * 
//     * To deal with this issue, our mapper tasks write a little auxiliary
//     * metadata file (per task) that tells the job driver which taskId processed
//     * which split#. Once the mapper-only job is completed, the job driver
//     * renames the output dirs such that the dir name contains the true solr
//     * shard id, based on these auxiliary files.
//     * 
//     * This way each doc gets assigned to the right Solr shard even with
//     * #reducers > #solrshards
//     * 
//     * Example for a merge with two shards:
//     * 
//     * part-m-00000 and part-m-00001 goes to outputShardNum = 0 and will end up
//     * in merged part-m-00000 part-m-00002 and part-m-00003 goes to
//     * outputShardNum = 1 and will end up in merged part-m-00001 part-m-00004
//     * and part-m-00005 goes to outputShardNum = 2 and will end up in merged
//     * part-m-00002 ... and so on
//     * 
//     * Also see run() method above where it uses
//     * NLineInputFormat.setNumLinesPerSplit(job, options.fanout)
//     * 
//     * Also see
//     * TreeMergeOutputFormat.TreeMergeRecordWriter.writeShardNumberFile()
//     */
//    private boolean renameTreeMergeShardDirs(Path outputTreeMergeStep, Job job,
//            FileSystem fs) throws IOException {
//        final String dirPrefix = SolrOutputFormat.getOutputName(job);
//        FileStatus[] dirs = fs.listStatus(outputTreeMergeStep,
//                new PathFilter() {
//                    @Override
//                    public boolean accept(Path path) {
//                        return path.getName().startsWith(dirPrefix);
//                    }
//                });
//
//        for (FileStatus dir : dirs) {
//            if (!dir.isDirectory()) {
//                throw new IllegalStateException("Not a directory: "
//                        + dir.getPath());
//            }
//        }
//
//        // Example: rename part-m-00004 to _part-m-00004
//        for (FileStatus dir : dirs) {
//            Path path = dir.getPath();
//            Path renamedPath = new Path(path.getParent(), "_" + path.getName());
//            if (!rename(path, renamedPath, fs)) {
//                return false;
//            }
//        }
//
//        // Example: rename _part-m-00004 to part-m-00002
//        for (FileStatus dir : dirs) {
//            Path path = dir.getPath();
//            Path renamedPath = new Path(path.getParent(), "_" + path.getName());
//
//            // read auxiliary metadata file (per task) that tells which taskId
//            // processed which split# aka solrShard
//            Path solrShardNumberFile = new Path(renamedPath,
//                    TreeMergeMapper.SOLR_SHARD_NUMBER);
//            InputStream in = fs.open(solrShardNumberFile);
//            byte[] bytes = ByteStreams.toByteArray(in);
//            in.close();
//            Preconditions.checkArgument(bytes.length > 0);
//            int solrShard = Integer.parseInt(new String(bytes, Charsets.UTF_8));
//            if (!delete(solrShardNumberFile, false, fs)) {
//                return false;
//            }
//
//            // same as FileOutputFormat.NUMBER_FORMAT
//            NumberFormat numberFormat = NumberFormat
//                    .getInstance(Locale.ENGLISH);
//            numberFormat.setMinimumIntegerDigits(5);
//            numberFormat.setGroupingUsed(false);
//            Path finalPath = new Path(renamedPath.getParent(), dirPrefix
//                    + "-m-" + numberFormat.format(solrShard));
//
//            LOG.info("MTree merge renaming solr shard: " + solrShard
//                    + " from dir: " + dir.getPath() + " to dir: " + finalPath);
//            if (!rename(renamedPath, finalPath, fs)) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//    private boolean waitForCompletion(Job job, boolean isVerbose)
//            throws IOException, InterruptedException, ClassNotFoundException {
//
//        LOG.debug("Running job: " + getJobInfo(job));
//        boolean success = job.waitForCompletion(isVerbose);
//        if (!success) {
//            LOG.error("Job failed! " + getJobInfo(job));
//        }
//        return success;
//    }
//
//    private void goodbye(Job job, long startTime) {
//        float secs = (System.nanoTime() - startTime) / (float) (10 ^ 9);
//        if (job != null) {
//            LOG.info("Succeeded with job: " + getJobInfo(job));
//        }
//        LOG.info("Success. Done. Program took {} secs. Goodbye.", secs);
//    }
//
//    private String getJobInfo(Job job) {
//        return "jobName: " + job.getJobName() + ", jobId: " + job.getJobID();
//    }
//
//    private boolean rename(Path src, Path dst, FileSystem fs)
//            throws IOException {
//        boolean success = fs.rename(src, dst);
//        if (!success) {
//            LOG.error("Cannot rename " + src + " to " + dst);
//        }
//        return success;
//    }
//
//    private boolean delete(Path path, boolean recursive, FileSystem fs)
//            throws IOException {
//        boolean success = fs.delete(path, recursive);
//        if (!success) {
//            LOG.error("Cannot delete " + path);
//        }
//        return success;
//    }
//
//    // same as IntMath.divide(p, q, RoundingMode.CEILING)
//    private long ceilDivide(long p, long q) {
//        long result = p / q;
//        if (p % q != 0) {
//            result++;
//        }
//        return result;
//    }
//
//    /**
//     * Returns <tt>log<sub>base</sub>value</tt>.
//     */
//    private double log(double base, double value) {
//        return Math.log(value) / Math.log(base);
//    }
//
// }
