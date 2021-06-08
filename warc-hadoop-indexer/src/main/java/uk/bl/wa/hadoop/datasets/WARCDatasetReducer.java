package uk.bl.wa.hadoop.datasets;

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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({ "deprecation" })
public class WARCDatasetReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    private static Logger log = LoggerFactory.getLogger(WARCDatasetReducer.class);


    static enum MyCounters {
        NUM_RECORDS, NUM_ERRORS, NUM_DROPPED_RECORDS
    }

    public WARCDatasetReducer() {
        try {
            Properties props = new Properties();
            props.load(getClass().getResourceAsStream(
                    "/log4j-override.properties"));
            PropertyConfigurator.configure(props);
        } catch (IOException e1) {
            log.error("Failed to load log4j config from properties file.");
        }
    }

    /**
     * Sets up our SolrServer. Presumes the existence of either
     * "warc.solr.zookepers" or "warc.solr.servers" in the config.
     */
    @Override
    public void configure(JobConf job) {
        log.info("Configuring reducer...");
        log.info("Initialisation complete.");
    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        Text wsr;

        // Go through the documents for this shard:
        long noValues = 0;
        while (values.hasNext()) {
            wsr = values.next();
            output.collect(key, wsr);
            noValues++;

            // Occasionally update application-level status:
            if ((noValues % 1000) == 0) {
                reporter.setStatus("Processed "
                        + noValues
                        + ", dropped "
                        + reporter.getCounter(MyCounters.NUM_DROPPED_RECORDS)
                                .getValue());
            }
        }

    }

    @Override
    public void close() {
    }

}
