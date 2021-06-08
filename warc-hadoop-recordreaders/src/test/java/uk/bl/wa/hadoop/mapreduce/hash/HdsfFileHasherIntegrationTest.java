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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;


import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.mapreduce.MapReduceTestBaseClass;

/**
 * 
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class HdsfFileHasherIntegrationTest extends MapReduceTestBaseClass {
    
    private static final Logger log = LoggerFactory.getLogger(HdsfFileHasherIntegrationTest.class);

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
                        assertTrue(line.contains(
                                "722eb9d7bfeb0b2ad2dd9c8a2fd7105f2880b139e5248e9b13a41d69ec63893b9afc034751be1432d867e171f4c6293ac89fc4e85c09a72288c16fd40f5996b2"));
                        assertTrue(line.contains("26164"));
                        assertTrue(line.contains(
                                "inputs/IAH-20080430204825-00000-blackbook-truncated.warc.gz"));
                    } else if (line_count == 2) {
                        assertTrue(line.contains(
                                "ba14747ac52ff1885905022299b4c470ad87270128939001b674c13e8787612011b4f2bd4f3c568df3b6789b7aa50ba0062c58a506debc12c57c037d10012203"));
                        assertTrue(line.contains("18406"));
                        assertTrue(line.contains(
                                "inputs/IAH-20080430204825-00000-blackbook-truncated.arc.gz"));
                    }
                }
                reader.close();
            } else {
                log.info(" --- ...skipping directory...");
            }
        }
    }

}
