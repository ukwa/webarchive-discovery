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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TinyCDXSender {

    private static final Logger log = LoggerFactory.getLogger(TinyCDXSender.class);

    // The tinycdxserver URL to POST to.
    private String endpoint;

    // The batch size
    private int batch_size;

    // The batch to build up and post
    private List<String> batch = new ArrayList<String>();

    // Total:
    private long total_records = 0;

    // Total Send:
    private long total_sent_records = 0;

    // Total failures:
    private long total_failures = 0;

    public TinyCDXSender(String endpoint, int batch_size) {
        this.endpoint = endpoint;
        this.batch_size = batch_size;
    }

    public void add(Text value) {
        // Add to the batch:
        batch.add(value.toString());
        total_records++;

        // Send if we're ready:
        if (batch.size() >= batch_size) {
            send_batch();
        }

    }

    private void send_batch() {
        boolean retry = true;
        int failures = 0;
        while (retry) {
            try {
                // POST to the endpoint:
                URL u = new URL(endpoint);
                HttpURLConnection conn = (HttpURLConnection) u.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type",
                        "application/x-www-form-urlencoded");
                OutputStream os = conn.getOutputStream();
                for (String t : this.batch) {
                    String line = t + "\n";
                    os.write(line.getBytes("UTF-8"));
                }
                os.close();
                if (conn.getResponseCode() == 200) {
                    // Read the response:
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(conn.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                    in.close();
                    log.info(response.toString());
                    // It worked! No need to retry:
                    log.info("Sent " + batch.size() + " records.");
                    this.total_sent_records += batch.size();
                    batch.clear();
                    retry = false;
                } else {
                    log.warn("Got response code: " + conn.getResponseCode());
                    failures += 1;
                    log.warn("Sleeping for 30s before retrying...");
                    Thread.sleep(1000 * 30);
                }
            } catch (Exception e) {
                log.warn("POSTing failed with ", e);
                failures++;
                total_failures++;
                log.warn("Sleeping for 30s before retrying...");
                try {
                    Thread.sleep(1000 * 30);
                } catch (InterruptedException e1) {
                    log.error("Sleep interrupted.");
                }
            }
            // Crash out if 10 attempts all failed (CDX Server it likely down):
            if (failures > 10) {
                throw new RuntimeException(
                        "Failed to post data to CDX server after " + failures
                                + " attempts!");
            }
        }
    }

    public void close() {
        if (this.batch.size() > 0) {
            this.send_batch();
        }
    }

    /**
     * @return the endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * @param endpoint
     *            the endpoint to set
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * @return the batch_size
     */
    public int getBatch_size() {
        return batch_size;
    }

    /**
     * @param batch_size
     *            the batch_size to set
     */
    public void setBatch_size(int batch_size) {
        this.batch_size = batch_size;
    }

    /**
     * 
     * @return
     */
    public long getTotalRecords() {
        return this.total_records;
    }

    /**
     * 
     * @return
     */
    public long getTotalSentRecords() {
        return this.total_sent_records;
    }

    /**
     * @return the total_failures
     */
    public long getTotalFailures() {
        return total_failures;
    }

}
