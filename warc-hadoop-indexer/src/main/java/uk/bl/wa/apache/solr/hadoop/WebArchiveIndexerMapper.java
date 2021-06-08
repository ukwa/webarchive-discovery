package uk.bl.wa.apache.solr.hadoop;

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
///**
// * 
// */
//package org.apache.solr.hadoop;
//
//import java.io.IOException;
//import java.security.NoSuchAlgorithmException;
//import java.util.Properties;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.apache.hadoop.io.Text;
//import org.apache.log4j.PropertyConfigurator;
//import org.archive.io.ArchiveRecordHeader;
//
//import uk.bl.wa.hadoop.WritableArchiveRecord;
//import uk.bl.wa.hadoop.indexer.WARCIndexerRunner;
//import uk.bl.wa.indexer.WARCIndexer;
//import uk.bl.wa.solr.SolrFields;
//import uk.bl.wa.solr.SolrRecord;
//
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigFactory;
//
///**
// * @author Andrew Jackson <Andrew.Jackson@bl.uk>
// *
// */
//public class WebArchiveIndexerMapper extends
//        SolrMapper<Text, WritableArchiveRecord> {
//    private static final Logger LOG = LoggerFactory
//            .getLogger(WebArchiveIndexerMapper.class);
//
//    private WARCIndexer windex;
//
//    public WebArchiveIndexerMapper() {
//        try {
//            Properties props = new Properties();
//            props.load(getClass().getResourceAsStream(
//                    "/log4j-override.properties"));
//            PropertyConfigurator.configure(props);
//        } catch (IOException e1) {
//            LOG.error("Failed to load log4j config from properties file.");
//        }
//
//    }
//    /*
//     * (non-Javadoc)
//     * 
//     * @see
//     * org.apache.solr.hadoop.SolrMapper#setup(org.apache.hadoop.mapreduce.Mapper
//     * .Context)
//     */
//    @Override
//    protected void setup(Context context)
//            throws IOException, InterruptedException {
//        super.setup(context);
//
//        // Additional...
//        try {
//            // Get config from job property:
//            Config config = ConfigFactory.parseString(context
//                    .getConfiguration()
//                    .get(WARCIndexerRunner.CONFIG_PROPERTIES));
//            // Initialise indexer:
//            this.windex = new WARCIndexer(config);
//            // Re-configure logging:
//        } catch (NoSuchAlgorithmException e) {
//            LOG.error("ArchiveTikaMapper.configure(): " + e.getMessage());
//        }
//
//    }
//
//    /*
//     * (non-Javadoc)
//     * 
//     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
//     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
//     */
//    @Override
//    protected void map(Text key, WritableArchiveRecord value, Context context)
//            throws IOException, InterruptedException {
//        ArchiveRecordHeader header = value.getRecord().getHeader();
//
//        if (!header.getHeaderFields().isEmpty()) {
//            SolrRecord solr = windex.extract(key.toString(), value.getRecord());
//
//            if (solr == null) {
//                LOG.debug("WARCIndexer returned NULL for: " + header.getUrl());
//                return;
//            }
//
//            String host = (String) solr.getFieldValue(SolrFields.SOLR_HOST);
//            if (host == null) {
//                host = "unknown.host";
//            }
//
//            Text oKey = new Text(host);
//            try {
//                SolrInputDocumentWritable wsolr = new SolrInputDocumentWritable(
//                        solr.getSolrDocument());
//                context.write(oKey, wsolr);
//            } catch (Exception e) {
//                LOG.error(e.getClass().getName() + ": " + e.getMessage() + "; "
//                        + header.getUrl() + "; " + oKey + "; " + solr);
//            }
//        }
//    }
//
// }
