/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.warcstats;

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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.ProtocolException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;
import org.archive.util.ArchiveUtils;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.hadoop.mapreduce.mdx.MDX;

/**
 * @author Andrew.Jackson@bl.uk
 *
 */
public class WARCRawStatsMapper extends MapReduceBase
        implements Mapper<Text, WritableArchiveRecord, Text, Text> {

    private static Logger log = LoggerFactory.getLogger(WARCRawStatsMapper.class);

    @Override
    public void map(Text key, WritableArchiveRecord value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        ArchiveRecord record = value.getRecord();
        ArchiveRecordHeader header = record.getHeader();

        // The header.url() might be encapsulated in '<>', which normally gives problems as they are passed back
        // when using header.getUrl(), but this code looks like stats extraction, so we leave them be.

        // Logging for debug info:
        log.debug("Processing @" + header.getOffset() + "+" + record.available()
                + "," + header.getLength() + ": " + header.getUrl());
        for (String h : header.getHeaderFields().keySet()) {
            log.debug(
                    "ArchiveHeader: " + h + " -> " + header.getHeaderValue(h));
        }

        try {
            MDX mdx = new MDX();
            Date crawl_date = ArchiveUtils
                    .parse14DigitISODate(header.getDate(), null);
            if (crawl_date != null) {
                mdx.setTs(ArchiveUtils.get14DigitDate(crawl_date));
            } else {
                mdx.setTs(header.getDate());
            }
            mdx.setUrl(header.getUrl());
            mdx.setHash(header.getDigest());

            // Data from WARC record:
            mdx.put("source-file", key.toString());
            mdx.put("content-type", header.getMimetype());
            mdx.put("content-length", "" + header.getContentLength());
            mdx.put("length", "" + header.getLength());
            mdx.put("source-offset", "" + header.getOffset());
            mdx.put("record-identifier", header.getRecordIdentifier());
            for (String k : header.getHeaderFieldKeys()) {
                mdx.put("HEADER-" + k, "" + header.getHeaderValue(k));
            }

            // check record type and look for HTTP data:
            Header[] httpHeaders = null;
            if (record instanceof WARCRecord) {
                mdx.setRecordType(
                        "warc." + header.getHeaderValue(HEADER_KEY_TYPE));
                mdx.setHash("" + header.getHeaderValue(
                        WARCConstants.HEADER_KEY_PAYLOAD_DIGEST));
                // There are not always headers! The code should check first.
                String statusLine = HttpParser.readLine(record, "UTF-8");
                if (statusLine != null && statusLine.startsWith("HTTP")) {
                    String firstLine[] = statusLine.split(" ");
                    if (firstLine.length > 1) {
                        String statusCode = firstLine[1].trim();
                        mdx.put("status-code", statusCode);
                        try {
                            httpHeaders = HttpParser.parseHeaders(record,
                                    "UTF-8");
                        } catch (ProtocolException p) {
                            log.error(
                                    "ProtocolException [" + statusCode + "]: "
                                            + header.getHeaderValue(
                                                    WARCConstants.HEADER_KEY_FILENAME)
                                            + "@"
                                            + header.getHeaderValue(
                                                    WARCConstants.ABSOLUTE_OFFSET_KEY),
                                    p);
                        }
                    } else {
                        log.warn("Could not parse status line: " + statusLine);
                    }
                } else {
                    log.warn("Invalid status line: "
                            + header.getHeaderValue(
                                    WARCConstants.HEADER_KEY_FILENAME)
                            + "@" + header.getHeaderValue(
                                    WARCConstants.ABSOLUTE_OFFSET_KEY));
                }

            } else if (record instanceof ARCRecord) {
                mdx.setRecordType("arc");
                ARCRecord arcr = (ARCRecord) record;
                mdx.put("status-code", "" + arcr.getStatusCode());
                httpHeaders = arcr.getHttpHeaders();

            } else {
                mdx.setRecordType("unknown");
            }

            // Add in http headers
            if (httpHeaders != null) {
                for (Header h : httpHeaders) {
                    mdx.put("HTTP-" + h.getName(), h.getValue());
                }
            }

            // URL:
            String uri = header.getUrl();
            if (uri != null) {
                UsableURI uuri = UsableURIFactory.getInstance(uri);
                // Hosts:
                if ("https".contains(uuri.getScheme())) {
                    mdx.put("host", uuri.getAuthority());
                }
            } else {
                mdx.put("errors", "malformed-url");
            }

            // Year
            String date = header.getDate();
            if (date != null && date.length() > 4) {
                mdx.put("year", date.substring(0, 4));
            } else {
                mdx.put("errors", "malformed-date");
            }

            // And collect:
            String outKey = mdx.getHash();
            if (outKey == null || outKey == "" || "null".equals(outKey)) {
                outKey = mdx.getRecordType() + ":" + header.getMimetype();
            } else {
                outKey = mdx.getRecordType() + ":" + outKey;
            }

            output.collect(new Text(outKey), new Text(mdx.toString()));
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
