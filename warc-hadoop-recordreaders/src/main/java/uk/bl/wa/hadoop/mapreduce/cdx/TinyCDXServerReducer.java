/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.cdx;

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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TinyCDXServerReducer
 extends Reducer<Text, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(TinyCDXServerReducer.class);

    private TinyCDXSender tcs;

    private long num_lines;
    private long num_dropped;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
     * Reducer.Context)
     */
    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        String endpoint = context.getConfiguration()
                .get("tinycdxserver.endpoint", "http://localhost:9090/test");
        log.warn("Sending to " + endpoint);
        int batch_size = context.getConfiguration()
                .getInt("tinycdxserver.batch_size", 10000);

        this.tcs = new TinyCDXSender(endpoint, batch_size);

        num_lines = 0;
        num_dropped = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text arg0, Iterable<Text> arg1,
            Reducer<Text, Text, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        for (Text t : arg1) {
            // Count lines:
            this.num_lines++;
            // Drop lines that appear to be raw HTTP header 200 responses
            // (OPTIONS requests, see
            // https://github.com/ukwa/webarchive-discovery/issues/215 -- this
            // is likely a rather specific to Twitter API calls but in general
            // we would expect HTTP 200 to have a real content type and not just
            // be HTTP headers):
            if (t.find(" application/http 200 ") > 0) {
                this.num_dropped++;
                continue;
            }
            // Drop lines that appear to be request or metadata records:
            if (t.find(" warc/request ") > 0 || t.find(" warc/metadata ") > 0) {
                this.num_dropped++;
                continue;
            }
            // Collect the rest:
            tcs.add(t);
        }
        // If we're running in produciton context:
        if (context != null) {
            // Record progress:
            context.setStatus("Seen " + tcs.getTotalRecords()
                    + " records, sent " + tcs.getTotalSentRecords() + "...");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.
     * Reducer.Context)
     */
    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        super.cleanup(context);
        tcs.close();
        // Update status:
        if (context != null) {
            context.setStatus("Seen " + tcs.getTotalRecords()
                    + " records, sent " + tcs.getTotalSentRecords() + "...");
            // And send totals:
            context.write(new Text("TOTAL_RECORDS"),
                    new Text("" + tcs.getTotalRecords()));
            context.write(new Text("TOTAL_SENT_RECORDS"),
                    new Text("" + tcs.getTotalSentRecords()));
            context.write(new Text("TOTAL_SEND_FAILURES"),
                    new Text("" + tcs.getTotalFailures()));
            context.write(new Text("TOTAL_LINES"),
                    new Text("" + this.num_lines));
            context.write(new Text("TOTAL_DROPPED_LINES"),
                    new Text("" + this.num_dropped));
        }
    }

}
