package uk.bl.wa.hadoop.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.After;
import org.junit.Before;

public abstract class MapReduceTestBaseClass {

    private static final Log log = LogFactory
            .getLog(MapReduceTestBaseClass.class);

    // Test cluster:
    protected MiniDFSCluster dfsCluster = null;
    protected MiniMRCluster mrCluster = null;

    // Input files:
    // 1. The variations.warc.gz example is rather large, and there are
    // mysterious problems parsing the statusCode.
    // 2. System can't cope with uncompressed inputs right now.
    protected final String[] testWarcs = new String[] {
            // "variations.warc.gz",
            // "IAH-20080430204825-00000-blackbook-truncated.arc",
            "IAH-20080430204825-00000-blackbook-truncated.arc.gz",
            // "IAH-20080430204825-00000-blackbook-truncated.warc",
            "IAH-20080430204825-00000-blackbook-truncated.warc.gz" };

    protected final Path input = new Path("inputs");
    protected final Path output = new Path("outputs");

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

    protected FileSystem getFileSystem() throws IOException {
        return dfsCluster.getFileSystem();
    }

    protected void createTextInputFile() throws IOException {
        OutputStream os = getFileSystem().create(new Path(input, "wordcount"));
        Writer wr = new OutputStreamWriter(os);
        wr.write("b a a\n");
        wr.close();
    }

    protected void copyFileToTestCluster(String filename, String localPrefix)
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
