package uk.bl.wa.hadoop.mapreduce;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This shared base class sets up a mini DFS/MR Hadoop cluster for testing, and
 * populates it with some test data.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class MapReduceTestBaseClass {

    private static final Logger log = LoggerFactory.getLogger(MapReduceTestBaseClass.class);

    // Test cluster:
    protected static MiniDFSCluster dfsCluster = null;
    protected static MiniMRCluster mrCluster = null;

    // Input files:
    // 1. The variations.warc.gz example is rather large, and there are
    // mysterious problems parsing the statusCode.
    // 2. System can't cope with uncompressed inputs right now.
    protected static final String[] testWarcs = new String[] {
            // "BL-20161016184836943-00270-28590~crawler03~8446.warc.gz",
            // "variations.warc.gz",
            // "IAH-20080430204825-00000-blackbook-truncated.arc",
            "IAH-20080430204825-00000-blackbook-truncated.arc.gz",
            // "IAH-20080430204825-00000-blackbook-truncated.warc",
            "IAH-20080430204825-00000-blackbook-truncated.warc.gz" };

    protected static final Path input = new Path("inputs");
    protected static final Path output = new Path("outputs");

    @BeforeClass
    public static void setUp() throws Exception {
        // static Print out the full config for debugging purposes:
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
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfsCluster.getFileSystem().makeQualified(input);
        dfsCluster.getFileSystem().makeQualified(output);
        //
        mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(),
                1);

        // prepare for tests
        for (String filename : testWarcs) {
            copyFileToTestCluster(filename,
                    "../warc-indexer/src/test/resources/");
        }

        log.warn("Spun up test cluster.");
    }

    protected static FileSystem getFileSystem() throws IOException {
        return dfsCluster.getFileSystem();
    }

    protected static void createTextInputFile() throws IOException {
        OutputStream os = getFileSystem().create(new Path(input, "wordcount"));
        Writer wr = new OutputStreamWriter(os);
        wr.write("b a a\n");
        wr.close();
    }

    protected static void copyFileToTestCluster(String filename,
            String localPrefix)
            throws IOException {
        Path targetPath = new Path(input, filename);
        File sourceFile = new File(localPrefix + filename);
        log.info("Copying " + filename + " into cluster at "
                + targetPath.toUri() + "...");
        FSDataOutputStream os = getFileSystem().create(targetPath);
        InputStream is = new FileInputStream(sourceFile);
        IOUtils.copy(is, os);
        is.close();
        os.close();
        log.info("Copy completed.");
    }

    @AfterClass
    public static void tearDown() throws Exception {
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

    /**
     * A simple test to check the setup worked:
     * 
     * @throws IOException
     */
    @Test
    public void testSetupWorked() throws IOException {
        log.info("Checking input file(s) is/are present...");
        // Check that the input file is present:
        Path[] inputFiles = FileUtil.stat2Paths(
                getFileSystem().listStatus(input, new OutputLogFilter()));
        Assert.assertEquals(testWarcs.length, inputFiles.length);
    }

}
