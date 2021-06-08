package uk.bl.wa.hadoop.indexer;

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
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.mapreduce.MapReduceTestBaseClass;

/**
 * 
 * 
 * @see https://wiki.apache.org/hadoop/HowToDevelopUnitTests
 * @see http://blog.pfa-labs.com/2010/01/unit-testing-hadoop-wordcount-example.html
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexerRunnerIntegrationTest extends MapReduceTestBaseClass {
    
    private static final Logger log = LoggerFactory.getLogger(WARCIndexerRunnerIntegrationTest.class);

    @SuppressWarnings( "deprecation" )
    @Test
    public void testFullIndexerJob() throws Exception {
        // prepare for test
        //createTextInputFile();

        log.info("Checking input file is present...");
        // Check that the input file is present:
        Path[] inputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
                input, new OutputLogFilter()));
        Assert.assertEquals(2, inputFiles.length);
        
        // Set up arguments for the job:
        // FIXME The input file could be written by this test.
        int reducers = 1;
        String[] args = { "--dummy-run", "-w", "-S", "http://none", "-R",
                ""
                + reducers,
                "-i",
                "src/test/resources/test-inputs.txt",
                "-o", this.output.getName() };
        
        // Set up the WARCIndexerRunner
        WARCIndexerRunner wir = new WARCIndexerRunner();

        // run job
        log.info("Setting up job config...");
        JobConf conf = this.mrCluster.createJobConf();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        wir.setConf(conf);
        log.info("Running job...");
        wir.run(args);
        log.info("Job finished, checking the results...");

        // check the output
        Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
                output, new OutputLogFilter()));
        Assert.assertEquals(reducers, outputFiles.length);
        
        // Check contents of the output:
        for( Path output : outputFiles ) {
            log.info(" --- output : "+output);
            if( getFileSystem().isFile(output) ) {
                InputStream is = getFileSystem().open(output);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String line = null;
                while( ( line = reader.readLine()) != null ) {
                    log.info(line);
                    System.out.println(line);
                }
                reader.close();
            } else {
                log.info(" --- ...skipping directory...");
            }
        }
    }

}
