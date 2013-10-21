package uk.bl.wa.hadoop.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.pdfbox.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 
 * 
 * @see https://wiki.apache.org/hadoop/HowToDevelopUnitTests
 * @see http://blog.pfa-labs.com/2010/01/unit-testing-hadoop-wordcount-example.html
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexerRunnerTest {
	
	private static final Log LOG = LogFactory.getLog(WARCIndexerRunnerTest.class);

	private MiniDFSCluster dfsCluster = null;
	private MiniMRCluster mrCluster = null;
	
	private final String testWarc = "../warc-indexer/src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz";

	private final Path input = new Path("inputs");
	private final Path output = new Path("outputs");

	@Before
	public void setUp() throws Exception {
		// Print out the full config for debugging purposes:
		//Config index_conf = ConfigFactory.load();
		//LOG.debug(index_conf.root().render());
		
		LOG.warn("Spinning up test cluster...");
		// make sure the log folder exists,
		// otherwise the test fill fail
		new File("target/test-logs").mkdirs();
		//
		System.setProperty("hadoop.log.dir", "target/test-logs");
		System.setProperty("javax.xml.parsers.SAXParserFactory",
				"com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
		
		//
		Configuration conf = new Configuration();
		dfsCluster = new MiniDFSCluster(conf, 1, true, null );
		dfsCluster.getFileSystem().makeQualified(input);
		dfsCluster.getFileSystem().makeQualified(output);
		//
		mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);
		
		// prepare for tests
		copyFileToTestCluster(testWarc, "test.warc.gz");
		
		LOG.warn("Spun up test cluster.");
	}

	protected FileSystem getFileSystem() throws IOException {
		return dfsCluster.getFileSystem();
	}

	private void createTextInputFile() throws IOException {
		OutputStream os = getFileSystem().create(new Path(input, "wordcount"));
		Writer wr = new OutputStreamWriter(os);
		wr.write("b a a\n");
		wr.close();
	}
	
	private void copyFileToTestCluster(String source, String target) throws IOException {
		Path targetPath = new Path(input, target);
		LOG.info("Copying "+source+" into cluster at "+targetPath.toUri()+"...");
		FSDataOutputStream os = getFileSystem().create(targetPath);
		InputStream is = new FileInputStream(source);
		IOUtils.copy(is, os);
		is.close();
		os.close();
		LOG.info("Copy completed.");
	}

	@Test
	public void testFileLoadWorked() throws Exception {
		LOG.info("Checking input file is present...");
		// Check that the input file is present:
		Path[] inputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
				input, new OutputLogFilter()));
		Assert.assertEquals(1, inputFiles.length);
	}
	
	@Test
	public void testFullIndexerJob() throws Exception {

		// prepare for test
		//createTextInputFile();
		
		// Set up arguments for the job:
		String[] args = {"src/test/resources/test-inputs.txt", this.output.getName()};
		
		// Set up the WARCIndexerRunner
		WARCIndexerRunner wir = new WARCIndexerRunner();

		// run job
		LOG.info("Setting up job config...");
		JobConf conf = this.mrCluster.createJobConf();
		wir.createJobConf(conf, args);
		LOG.info("Running job...");
		JobClient.runJob(conf);
		LOG.info("Job finished, checking the results...");

		// check the output
		Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
				output, new OutputLogFilter()));
		Assert.assertEquals(1, outputFiles.length);
		
		// Check the server has the documents? Requires an local Solr during testing.
		
		//InputStream is = getFileSystem().open(outputFiles[0]);
		//BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		//Assert.assertEquals("a\t2", reader.readLine());
		//Assert.assertEquals("b\t1", reader.readLine());
		//Assert.assertNull(reader.readLine());
		//reader.close();
	}

	@After
	public void tearDown() throws Exception {
		LOG.warn("Tearing down test cluster...");
		if (dfsCluster != null) {
			dfsCluster.shutdown();
			dfsCluster = null;
		}
		if (mrCluster != null) {
			mrCluster.shutdown();
			mrCluster = null;
		}
		LOG.warn("Torn down test cluster.");
	}

}
