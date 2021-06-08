/**
 * 
 */
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDXSeqSampleGeneratorIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(MDXSeqSampleGeneratorIntegrationTest.class);

    // Test cluster:
    private MiniDFSCluster dfsCluster = null;
    private MiniMRCluster mrCluster = null;

    // Input files:
    public final static String[] testWarcs = new String[] { "mdx-seq/mdx-warc-both.seq" };

    private final Path input = new Path("inputs");
    private final Path output = new Path("outputs");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        log.warn("Spinning up test cluster...");
        // make sure the log folder exists,
        // otherwise the test fill fail
        new File("target/test-logs").mkdirs();
        //
        System.setProperty("hadoop.log.dir", "target/test-logs");
        System.setProperty("javax.xml.parsers.SAXParserFactory",
                "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        //
        Configuration conf = new Configuration();
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfsCluster.getFileSystem().makeQualified(input);
        dfsCluster.getFileSystem().makeQualified(output);
        //
        mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri()
                .toString(), 1);

        // prepare for tests
        for (String filename : testWarcs) {
            WARCMDXGeneratorIntegrationTest.copyFileToTestCluster(
                    dfsCluster.getFileSystem(), input, "src/test/resources/",
                    filename);
        }

        log.warn("Spun up test cluster.");
    }

    @Test
    public void testSeqStats() throws Exception {
        log.info("Checking input file is present...");
        // Check that the input file is present:
        Path[] inputFiles = FileUtil
                .stat2Paths(dfsCluster.getFileSystem().listStatus(
                        new Path(input, "mdx-seq/"), new OutputLogFilter()));
        Assert.assertEquals(1, inputFiles.length);

        // Create a file of the inputs
        File tmpInputsFile = WARCMDXGeneratorIntegrationTest
                .writeInputFile(inputFiles);

        // Set up arguments for the job:
        String[] args = { "-i", tmpInputsFile.getAbsolutePath(), "-o",
                this.output.getName() };

        // Set up the WARCIndexerRunner
        MDXSeqSampleGenerator wir = new MDXSeqSampleGenerator();

        // run job
        // Job configuration:
        log.info("Setting up job config...");
        JobConf jobConf = this.mrCluster.createJobConf();
        wir.createJobConf(jobConf, args);
        log.info("Running job...");
        JobClient.runJob(jobConf);
        log.info("Job finished, checking the results...");

        // check the output exists
        Path[] outputFiles = FileUtil.stat2Paths(dfsCluster.getFileSystem()
                .listStatus(
                output, new OutputLogFilter()));
        // Assert.assertEquals(1, outputFiles.length);

        // Copy the output out:
        for (Path output : outputFiles) {
            FileOutputStream fout = new FileOutputStream("target/"
                    + output.getName());
            log.info(" --- output : " + output);
            if (dfsCluster.getFileSystem().isFile(output)) {
                InputStream is = dfsCluster.getFileSystem().open(output);
                IOUtils.copy(is, fout);
            } else {
                log.info(" --- ...skipping directory...");
            }
            fout.close();
        }

        // Check contents of the output:
        // TBA
    }

    @After
    public void tearDown() throws Exception {
        log.warn("Tearing down test cluster...");
        if (dfsCluster != null) {
            dfsCluster.shutdown();
            dfsCluster = null;
        }
        if (mrCluster != null) {
            mrCluster.shutdown();
            mrCluster = null;
        }
        log.warn("Torn down test cluster.");
    }

}
