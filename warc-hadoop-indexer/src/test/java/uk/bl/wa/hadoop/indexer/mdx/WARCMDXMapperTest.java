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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.hadoop.mapreduce.mdx.MDXReduplicatingReducer;
import uk.bl.wa.util.Normalisation;

public class WARCMDXMapperTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(WARCMDXMapperTest.class);

    MapDriver<Text, WritableArchiveRecord, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<Text, WritableArchiveRecord, Text, Text, Text, Text> mapReduceDriver;


    @Before
    public void setUp() {
        // Overload the config:
        Configuration conf = new Configuration();
        // Store the args in there, so Mapper/Reducer can read them:
        String[] args = new String[] { "-i", "in", "-o", "out", "-S", "none" };
        conf.set("commandline.args", String.join("@@@", args));        
        Config c = ConfigFactory.load("mdx");
        conf.set(WARCMDXGenerator.CONFIG_PROPERTIES, c.withOnlyPath("warc")
                .root().render(ConfigRenderOptions.concise()));
        // Set up the mapper etc.:
        WARCMDXMapper mapper = new WARCMDXMapper();
        MDXReduplicatingReducer reducer = new MDXReduplicatingReducer();
        mapDriver = MapDriver.newMapDriver(mapper).withConfiguration(conf);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver();
    }

    @Test
    public void testMapper() throws IOException, JSONException {

        Set<String> skippableRecords = new HashSet<String>();
        skippableRecords.add("application/warc-fields");
        skippableRecords.add("text/dns");

        File inputFile = new File(
                "../warc-indexer/src/test/resources/gov.uk-revisit-warcs/BL-20140325121225068-00000-32090~opera~8443.warc.gz");
        String archiveName = inputFile.getName();

        ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
        Iterator<ArchiveRecord> ir = reader.iterator();
        ArchiveRecord record;
        Text key = new Text();
        WritableArchiveRecord value = new WritableArchiveRecord();
        while (ir.hasNext()) {
            record = (ArchiveRecord) ir.next();
            key.set(archiveName);
            value.setRecord(record);

            LOG.info("GOT: " + record.getHeader().getRecordIdentifier());
            LOG.info("GOT: " + record.getHeader().getMimetype());
            // Skip records that can't be analysed:
            if (skippableRecords.contains(record.getHeader()
                    .getMimetype()))
                continue;

            // Run through them all:
            LOG.info("Running without testing output...");
            mapDriver.setInput(key, value);
            List<Pair<Text, Text>> result = mapDriver.run();
            if (result != null && result.size() > 0) {
                MDX mdx = new MDX(result.get(0).getSecond().toString());
                LOG.info("RESULT MDX: " + mdx);

                // Perform a specific check for one of the items:
                if ("http://data.gov.uk/".equals(Normalisation.sanitiseWARCHeaderValue(record.getHeader().getUrl()))
                                                 && record.getHeader().getMimetype()
                                .contains("response")) {
                    Text testKey = new Text(
                            "sha1:SKAVWVVB6HYPSTY3YNQJVM2C4FZRWBSG");
                    MDX testMdx = new MDX(
                            "{\"digest\":\"sha1:SKAVWVVB6HYPSTY3YNQJVM2C4FZRWBSG\",\"url\":\"http://data.gov.uk/\",\"timestamp\":\"20140325121238\"}");
                    assertEquals(testKey, result.get(0).getFirst());
                    assertEquals(testMdx.getUrl(), mdx.getUrl());
                    assertEquals(testMdx.getHash(), mdx.getHash());
                    assertEquals(testMdx.getTs(), mdx.getTs());
                }

            }
            mapDriver.resetOutput();
        }
    }

}
