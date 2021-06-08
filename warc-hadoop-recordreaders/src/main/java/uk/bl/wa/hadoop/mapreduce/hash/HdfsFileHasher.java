/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.hash;

/*
 * #%L
 * warc-hadoop-recordreaders
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.mapreduce.lib.input.UnsplittableInputFileFormat;

/*
 * Compute full file hashes at scale.
 * 
 * Works by using a special reader that breaks each file into a sequence of binary records but without
 * 'splitting' the file (in the Hadoop sense).  The chunks are read in sequence for each file, by key,
 * so the total hash computation works fine.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class HdfsFileHasher extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileHasher.class);

    private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-r <#reducers>] [-m] [Use MD5 instead of SHA-512.]";
    private static final String CLI_HEADER = "MapReduce job for calculating checksums of files on HDFS.";

    /* (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        // Options:
        String[] otherArgs = new GenericOptionsParser(args)
                .getRemainingArgs();

        // Process remaining args list this:
        Options options = new Options();
        options.addOption("i", true,
                "a local file containing a list of HDFS paths to process");
        options.addOption("o", true, "output directory");
        options.addOption("m", false, "use MD5 rather than SHA-512");
        options.addOption("r", true, "number of reducers (defaults to 1)");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, otherArgs);
        if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.setWidth(80);
            helpFormatter.printHelp(CLI_USAGE, CLI_HEADER, options, "");
            System.exit(1);
        }
        String input_file = cmd.getOptionValue("i");
        String output_path = cmd.getOptionValue("o");
        String algorithm = null;
        int numReducers = 1;
        if (cmd.hasOption("m")) {
            algorithm = "MD5";
        }
        if (cmd.hasOption("r")) {
            numReducers = Integer.parseInt(cmd.getOptionValue("r"));
        }

        // When implementing tool, choose algorithm:
        Configuration conf = this.getConf();
        if (algorithm != null)
            conf.set(MessageDigestMapper.CONFIG_DIGEST_ALGORITHM, algorithm);

        // Create job
        Job job = new Job(conf, "HDFS File Checksummer");
        job.setJarByClass(HdfsFileHasher.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(MessageDigestMapper.class);
        job.setReducerClass(Reducer.class);

        // Just one output file:
        job.setNumReduceTasks(numReducers);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        log.info("Reading input files...");
        String line = null;
        long line_count = 0;
        BufferedReader br = new BufferedReader(new FileReader(input_file));
        while ((line = br.readLine()) != null) {
            if (StringUtils.isEmpty(line))
                continue;
            //
            line_count++;
            Path path = new Path(line);
            FileSystem fs = path.getFileSystem(conf);
            if (fs.isFile(path)) {
                FileInputFormat.addInputPath(job, path);
            } else if (fs.isDirectory(path)) {
                FileStatus[] listing = fs.listStatus(path);
                int list_count = 0;
                for (FileStatus fstat : listing) {
                    list_count++;
                    log.info("Checking " + list_count + "/" + listing.length
                            + " " + fstat.getPath());
                    if (!fstat.isDir()) {
                        FileInputFormat.addInputPath(job, fstat.getPath());
                    }
                }
            }
        }
        br.close();
        log.info("Read " + FileInputFormat.getInputPaths(job).length
                + " input files from " + line_count + " paths.");
        job.setInputFormatClass(UnsplittableInputFileFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * This job takes two arguments: <local-input-list> <hdfs-output-dir>
     * 
     * The first is a pointer to a local file containing a list of HDFS paths to
     * hash.
     * 
     * The second is an output directory to store the results of the hashing
     * process in.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HdfsFileHasher(),
                args);
        System.exit(res);
    }

}
