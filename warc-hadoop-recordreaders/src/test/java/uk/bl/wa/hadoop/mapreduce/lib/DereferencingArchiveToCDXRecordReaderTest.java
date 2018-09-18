package uk.bl.wa.hadoop.mapreduce.lib;

/*
 * #%L
 * warc-hadoop-recordreaders
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

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;

public class DereferencingArchiveToCDXRecordReaderTest {

    private static final Log log = LogFactory
            .getLog(DereferencingArchiveToCDXRecordReaderTest.class);

    private void runCDXTest(Configuration conf, String expected)
            throws Exception {
        File testFile = new File("src/test/resources/rr-test-inputs.txt");
        Path path = new Path(testFile.getAbsoluteFile().toURI().toString());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        ArchiveToCDXFileInputFormat inputFormat = ReflectionUtils
                .newInstance(ArchiveToCDXFileInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContext(conf,
                new TaskAttemptID());
        RecordReader<Text, Text> reader = inputFormat.createRecordReader(split,
                context);

        reader.initialize(split, context);

        int position = 0;
        String value = "";
        while (reader.nextKeyValue() != false) {
            position += 1;
            if (position == 3)
                value = reader.getCurrentValue().toString();
        }
        // Check the third value is as expected
        log.debug(value);
        Assert.assertEquals(expected, value);
    }

    @Test
    public void testCDX11() throws Exception {

        Configuration conf = new Configuration();
        
        conf.set("cdx.format", DereferencingArchiveToCDXRecordReader.CDX_11);
        conf.setBoolean("cdx.hdfs", false);
        
        this.runCDXTest(conf,
                "archive.org/robots.txt 20080430204825 http://www.archive.org/robots.txt text/plain 200 SUCGMUVXDKVB5CS2NL4R4JABNX7K466U - - - 776 IAH-20080430204825-00000-blackbook-truncated.arc.gz");

    }

    @Test
    public void testCDX11HdfsPath() throws Exception {

        Configuration conf = new Configuration();

        conf.set("cdx.format", DereferencingArchiveToCDXRecordReader.CDX_11);
        conf.setBoolean("cdx.hdfs", true);

        this.runCDXTest(conf,
                "archive.org/robots.txt 20080430204825 http://www.archive.org/robots.txt text/plain 200 SUCGMUVXDKVB5CS2NL4R4JABNX7K466U - - - 776 ../warc-indexer/src/test/resources/IAH-20080430204825-00000-blackbook-truncated.arc.gz");


    }

}

