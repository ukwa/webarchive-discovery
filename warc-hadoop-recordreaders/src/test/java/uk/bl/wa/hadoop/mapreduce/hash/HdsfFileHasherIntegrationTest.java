package uk.bl.wa.hadoop.mapreduce.hash;

import static org.junit.Assert.assertEquals;

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

import uk.bl.wa.hadoop.mapreduce.MapReduceTestBaseClass;

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
        String[] args = { "-i", "src/test/resources/test-input-dir.txt", "-o",
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
		int line_count = 0;
		for( Path output : outputFiles ) {
			log.info(" --- output : "+output);
			if( getFileSystem().isFile(output) ) {
				InputStream is = getFileSystem().open(output);
				BufferedReader reader = new BufferedReader(new InputStreamReader(is));
				String line = null;
				while( ( line = reader.readLine()) != null ) {
					log.info(line);
                    line_count++;
                    // Check:
                    if (line_count == 1) {
                        assertEquals(
                                "/user/andy/inputs\t722eb9d7bfeb0b2ad2dd9c8a2fd7105f2880b139e5248e9b13a41d69ec63893b9afc034751be1432d867e171f4c6293ac89fc4e85c09a72288c16fd40f5996b2 26164 /user/andy/inputs/IAH-20080430204825-00000-blackbook-truncated.warc.gz",
                                line);
                    } else if (line_count == 2) {
                        assertEquals(
                                "/user/andy/inputs\tba14747ac52ff1885905022299b4c470ad87270128939001b674c13e8787612011b4f2bd4f3c568df3b6789b7aa50ba0062c58a506debc12c57c037d10012203 18406 /user/andy/inputs/IAH-20080430204825-00000-blackbook-truncated.arc.gz",
                                line);
                    }
				}
				reader.close();
			} else {
				log.info(" --- ...skipping directory...");
			}
		}
	}

}
