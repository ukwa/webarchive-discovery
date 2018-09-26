package uk.bl.wa.hadoop.mapreduce.nlp;

/*-
 * #%L
 * warc-nlp
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;

import uk.bl.wa.hadoop.mapreduce.mdx.MDX;

@SuppressWarnings({ "deprecation" })
public class Word2VecReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    private static Log log = LogFactory.getLog(Word2VecReducer.class);

    static enum MyCounters {
        NUM_RECORDS, NUM_REVISITS, NUM_ERRORS, NUM_DROPPED_RECORDS, NUM_UNRESOLVED_REVISITS, NUM_RESOLVED_REVISITS
    }

    private static final Text revisit = new Text("revisit");
    private static final Text response = new Text("response");

    public Word2VecReducer() {
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
     */
    @Override
    public void configure(JobConf job) {
        log.info("Initialisation complete.");
    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
                    throws IOException {

        long noValues = 0;
        MDX mdx;
        MDX exemplar = null;
        List<MDX> toReduplicate = new ArrayList<MDX>();
        while (values.hasNext()) {
            mdx = new MDX(values.next().toString());
            noValues++;
            
            // Collect the revisit records:
            if (revisit.equals(mdx.getRecordType())) {
                // Add this revisit record to the stack:
                reporter.incrCounter(MyCounters.NUM_REVISITS, 1);
                toReduplicate.add(mdx);
            } else {
                // Record a response record:
                if (exemplar == null && response.equals(mdx.getRecordType())) {
                    exemplar = mdx;
                }
                // Collect complete records:
                Text outKey = new Text(mdx.getHash());
                output.collect(outKey, new Text(mdx.toString()));
            }
            
            // Report:
            reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

            // Occasionally update status report:
            if ((noValues % 1000) == 0) {
                reporter.setStatus("Processed "
                        + noValues
                        + ", of which "
                        + reporter.getCounter(MyCounters.NUM_REVISITS)
                                .getValue() + " records need reduplication.");
            }

        }
        
        // Now fix up revisits:
        for (MDX rmdxw : toReduplicate) {
            // Set outKey based on hash:
            Text outKey = new Text(rmdxw.getHash());
            // Handle merge:
            if( exemplar != null ) {
                // Modify record type and and merge the properties:
                rmdxw.setRecordType("reduplicated");
                //rmdxw..getProperties().putAll(exemplar.getProperties());
                reporter.incrCounter(MyCounters.NUM_RESOLVED_REVISITS, 1);
                // Collect resolved records:
                output.collect(outKey, new Text(rmdxw.toString()));
            } else {
                reporter.incrCounter(MyCounters.NUM_UNRESOLVED_REVISITS, 1);
                // Collect unresolved records:
                output.collect(outKey, new Text(rmdxw.toString()));
            }
        }

    }

}
