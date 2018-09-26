package uk.bl.wa.hadoop.indexer.mdx;

/*
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2018 The webarchive-discovery project contributors
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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.hadoop.mapreduce.mdx.MDXSeqMerger;

public class WARCMDXGeneratorIntegrationTest {

    private static final Log log = LogFactory
            .getLog(WARCMDXGeneratorIntegrationTest.class);

    // Test cluster:
    private MiniDFSCluster dfsCluster = null;
    private MiniMRCluster mrCluster = null;

    // Input files:
    public final static String[] testWarcs = new String[] {
            "gov.uk-revisit-warcs/BL-20140325121225068-00000-32090~opera~8443.warc.gz",
            "gov.uk-revisit-warcs/BL-20140325122341434-00000-32090~opera~8443.warc.gz" };

    private final Path input = new Path("inputs");
    private final Path output = new Path("outputs");
    private final Path outputMerged = new Path("outputs-merged");

    // Exported results
    public static File outputSeq = new File("target/test.seq");
    public static File outputMergedSeq = new File("target/test-merged.seq");

    @Before
    public void setUp() throws Exception {
        // Print out the full config for debugging purposes:
        // Config index_conf = ConfigFactory.load();
        // LOG.debug(index_conf.root().render());

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
        System.setProperty("test.build.data",
                new File("target/mini-dfs").getAbsolutePath());
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfsCluster.getFileSystem().makeQualified(input);
        dfsCluster.getFileSystem().makeQualified(output);
        //
        mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);

        // prepare for tests
        for (String filename : testWarcs) {
            copyFileToTestCluster(getFileSystem(), input,
                    "../warc-indexer/src/test/resources/", filename);
        }

        log.warn("Spun up test cluster.");
    }

    protected FileSystem getFileSystem() throws IOException {
        return dfsCluster.getFileSystem();
    }

    public static void copyFileToTestCluster(FileSystem fs, Path input,
            String prefix, String filename)
            throws IOException {
        Path targetPath = new Path(input, filename);
        File sourceFile = new File(prefix + filename);
        log.info("Copying " + filename + " into cluster at "
                + targetPath.toUri() + "...");
        FSDataOutputStream os = fs.create(targetPath);
        InputStream is = new FileInputStream(sourceFile);
        IOUtils.copy(is, os);
        is.close();
        os.close();
        log.info("Copy completed.");
    }

    public static File writeInputFile(Path[] inputFiles) throws Exception {
        // Make a list:
        File tmpInputsFile = File.createTempFile("inputs", ".txt");
        tmpInputsFile.deleteOnExit();
        Writer s = new FileWriter(tmpInputsFile);
        for (Path p : inputFiles) {
            s.write(p.toString() + "\n");
        }
        s.close();
        return tmpInputsFile;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMDXGenerator() throws Exception {
        // prepare for test
        // createTextInputFile();

        log.info("Checking input file is present...");
        // Check that the input file is present:
        Path[] inputFiles = FileUtil.stat2Paths(getFileSystem()
                .listStatus(new Path(input, "gov.uk-revisit-warcs/"),
                        new OutputLogFilter()));
        Assert.assertEquals(2, inputFiles.length);
        // Create a file of the inputs
        File tmpInputsFile = writeInputFile(inputFiles);

        // Set up arguments for the job:
        String[] args = { "-i", tmpInputsFile.getAbsolutePath(), "-o",
                this.output.getName() };

        // Set up the WARCIndexerRunner
        WARCMDXGenerator wir = new WARCMDXGenerator();

        // run job
        // Job configuration:
        log.info("Setting up job config...");
        JobConf jobConf = this.mrCluster.createJobConf();
        jobConf.setInt(WARCMDXGenerator.WARC_HADOOP_NUM_REDUCERS, 1);
        jobConf.set("mapred.child.java.opts", "-Xmx512m");
        wir.createJobConf(jobConf, args);
        log.info("Running job...");
        JobClient.runJob(jobConf);
        log.info("Job finished, checking the results...");

        // check the output exists
        Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
                output, new OutputLogFilter()));
        // Default is 1 reducers (as knitting together multiple sequence files
        // is not a mere matter of concatentation):
        Assert.assertEquals(1, outputFiles.length);

        // Copy the output out of HDFS and onto local FS:
        FileOutputStream fout = new FileOutputStream(outputSeq);
        for (Path output : outputFiles) {
            log.info(" --- output : " + output);
            if (getFileSystem().isFile(output)) {
                InputStream is = getFileSystem().open(output);
                IOUtils.copy(is, fout);
            } else {
                log.info(" --- ...skipping directory...");
            }
            fout.flush();
        }
        fout.close();

        // Check contents of the output:
        Configuration config = new Configuration();
        Path path = new Path(outputSeq.getAbsolutePath());
        SequenceFile.Reader reader = new SequenceFile.Reader(
                FileSystem.get(config), path, config);
        WritableComparable key = (WritableComparable) reader.getKeyClass()
                .newInstance();
        Writable value = (Writable) reader.getValueClass().newInstance();

        MDX mdx;
        int counter = 0;
        while (reader.next(key, value)) {
            mdx = new MDX(value.toString());
            System.out.println("Key is: " + key + " record_type: "
                    + mdx.getRecordType() + " SURT: " + mdx.getUrlAsSURT());
            counter++;
        }
        assertEquals(114, counter);
        reader.close();
        
        // Now test the MDXSeqMerger
        testSeqMerger(outputFiles);
    }

    private void testSeqMerger(Path[] inputFiles) throws Exception {

        // Create a file of the inputs
        File tmpInputsFile = writeInputFile(inputFiles);

        // Set up arguments for the job:
        String[] args = { "-i", tmpInputsFile.getAbsolutePath(), "-o",
                this.outputMerged.getName(), "-r", "1" };

        // Set up the WARCIndexerRunner
        MDXSeqMerger msm = new MDXSeqMerger();

        // run job
        log.info("Setting up job config...");
        JobConf jobConf = this.mrCluster.createJobConf();
        msm.createJobConf(jobConf, args);
        log.info("Running job...");
        JobClient.runJob(jobConf);
        log.info("Job finished, checking the results...");

        // Copy the output out of HDFS and onto local FS:
        FileOutputStream fout = new FileOutputStream(outputMergedSeq);
        Path[] outputFiles = FileUtil.stat2Paths(getFileSystem()
                .listStatus(outputMerged, new OutputLogFilter()));
        for (Path output : outputFiles) {
            log.info(" --- output : " + output);
            if (getFileSystem().isFile(output)) {
                InputStream is = getFileSystem().open(output);
                IOUtils.copy(is, fout);
            } else {
                log.info(" --- ...skipping directory...");
            }
            fout.flush();
        }
        fout.close();

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
