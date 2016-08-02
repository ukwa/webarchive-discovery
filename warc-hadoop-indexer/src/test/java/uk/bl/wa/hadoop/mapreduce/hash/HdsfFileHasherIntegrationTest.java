package uk.bl.wa.hadoop.mapreduce.hash;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

import uk.bl.wa.hadoop.MapReduceTestBaseClass;

/**
 * 
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class HdsfFileHasherIntegrationTest extends MapReduceTestBaseClass {
	
	private static final Log log = LogFactory.getLog(HdsfFileHasherIntegrationTest.class);

	@SuppressWarnings( "deprecation" )
	@Test
    public void testShaSumHasher() throws Exception {
		// prepare for test
		//createTextInputFile();

		log.info("Checking input file is present...");
		// Check that the input file is present:
		Path[] inputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
				input, new OutputLogFilter()));
		Assert.assertEquals(2, inputFiles.length);
		
		// Set up arguments for the job:
		// FIXME The input file could be written by this test.
        String[] args = { "src/test/resources/test-inputs.txt",
                this.output.getName() };
		
		// run job
		log.info("Setting up job config...");
		JobConf conf = this.mrCluster.createJobConf();
		log.info("Running job...");
        ToolRunner.run(conf, new HdfsFileHasher(), args);
		log.info("Job finished, checking the results...");

		// check the output
		Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
				output, new OutputLogFilter()));
		//Assert.assertEquals(1, outputFiles.length);
		
		// Check contents of the output:
		for( Path output : outputFiles ) {
			log.info(" --- output : "+output);
			if( getFileSystem().isFile(output) ) {
				InputStream is = getFileSystem().open(output);
				BufferedReader reader = new BufferedReader(new InputStreamReader(is));
				String line = null;
				while( ( line = reader.readLine()) != null ) {
					log.info(line);
				}
				reader.close();
			} else {
				log.info(" --- ...skipping directory...");
			}
		}
	}

}
