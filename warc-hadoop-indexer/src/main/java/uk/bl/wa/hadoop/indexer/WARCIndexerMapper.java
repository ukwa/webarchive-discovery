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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.SurtPrefixSet;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.annotation.Annotator;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.util.Normalisation;

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerMapper extends MapReduceBase implements
        Mapper<Text, WritableArchiveRecord, IntWritable, WritableSolrRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(WARCIndexerMapper.class );

    static enum MyCounters {
        NUM_RECORDS, NUM_ERRORS, NUM_NULLS, NUM_EMPTY_HEADERS
    }

    private String mapTaskId;
    private String inputFile;
    private int noRecords = 0;

    private WARCIndexer windex;

    private WARCIndexerOptions opts = new WARCIndexerOptions();

    private Config config;

    private SolrRecordFactory solrFactory = SolrRecordFactory.createFactory(null); // Overridden by innerConfigure

    public WARCIndexerMapper() {
        try {
            // Re-configure logging:
            Properties props = new Properties();
            props.load(getClass().getResourceAsStream("/log4j-override.properties"));
            PropertyConfigurator.configure(props);
        } catch (IOException e1) {
            LOG.error("Failed to load log4j config from properties file.");
        }
    }

    @Override
    public void configure( JobConf job ) {
        // Get config from args:
        String[] args = job.get("commandline.args").split("@@@");
        new CommandLine(opts).parseArgs(args);

        // Get config from job property:
        if (this.config == null) {
            innerConfigure(ConfigFactory.parseString(job
                .get(WARCIndexerRunner.CONFIG_PROPERTIES)));
        }

        // Configure annotations:
        this.configureAnnotations();

        // Other properties:
        mapTaskId = job.get("mapred.task.id");
        inputFile = job.get("map.input.file");
        LOG.info("Got task.id " + mapTaskId + " and input.file " + inputFile);

        // Set up a decent font cache location for PDFBox
        System.setProperty("pdfbox.fontcache", job.get("mapred.child.tmp"));
    }

    private void configureAnnotations() {
        try {
            // Decide whether to try to apply annotations:
            if (opts.annotations) {
                LOG.info(
                        "Attempting to load annotations from 'annotations.json'...");
                Annotations ann = Annotations.fromJsonFile("annotations.json");
                LOG.info(
                        "Attempting to load OA SURTS from 'openAccessSurts.txt'...");
                SurtPrefixSet oaSurts = Annotator
                        .loadSurtPrefix("openAccessSurts.txt");
                windex.setAnnotations(ann, oaSurts);
            }
        } catch (JsonParseException e) {
            LOG.error("WARCIndexerMapper.configure(): " + e.getMessage());
        } catch (JsonMappingException e) {
            LOG.error("WARCIndexerMapper.configure(): " + e.getMessage());
        } catch (IOException e) {
            LOG.error("WARCIndexerMapper.configure(): " + e.getMessage());
        }

    }

    public void innerConfigure(Config config) {
        try {
            // Initialise indexer:
            this.windex = new WARCIndexer( config );
            // Set up record factory:
            solrFactory = SolrRecordFactory.createFactory(config);
        } catch( NoSuchAlgorithmException e ) {
            LOG.error("WARCIndexerMapper.configure(): " + e.getMessage());
        }
    }

    public WritableSolrRecord innerMap(Text key,
            WritableArchiveRecord value,
            Reporter reporter) throws IOException {

        ArchiveRecordHeader header = value.getRecord().getHeader();

        noRecords++;

        ArchiveRecord rec = value.getRecord();
        SolrRecord solr = solrFactory.createRecord(key.toString(), rec.getHeader());
        final String url = Normalisation.sanitiseWARCHeaderValue(header.getUrl());
        try {
            if (!header.getHeaderFields().isEmpty()) {
                // Do the indexing:
                solr = windex.extract(key.toString(),
                        value.getRecord());

                // If there is no result, report it
                if (solr == null) {
                    LOG.debug("WARCIndexer returned NULL for: " + url);
                    reporter.incrCounter(MyCounters.NUM_NULLS, 1);
                    return null;
                }

                // Increment record counter:
                reporter.incrCounter(MyCounters.NUM_RECORDS, 1);

            } else {
                // Report headerless records:
                reporter.incrCounter(MyCounters.NUM_EMPTY_HEADERS, 1);

            }

        } catch (Exception e) {
            LOG.error(e.getClass().getName() + ": " + e.getMessage() + "; "
                    + url + "; " + header.getOffset(), e);
            // Increment error counter
            reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
            // Store it:
            solr.addParseException(e);

        } catch (OutOfMemoryError e) {
            // Allow processing to continue if a record causes OOME:
            LOG.error("OOME " + e.getClass().getName() + ": " + e.getMessage()
                    + "; " + url + "; " + header.getOffset());
            // Increment error counter
            reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
            // Store it:
            solr.addParseException(e);
        }

        // Wrap up and collect the result:
        WritableSolrRecord wsolr = new WritableSolrRecord(solr);

        // Occasionally update application-level status
        if ((noRecords % 1000) == 0) {
            reporter.setStatus(noRecords + " processed from " + inputFile);
            // Also assure framework that we are making progress:
            reporter.progress();
        }

        return wsolr;

    }

    @Override
    public void map(Text key, WritableArchiveRecord value,
            OutputCollector<IntWritable, WritableSolrRecord> output,
            Reporter reporter) throws IOException {

        WritableSolrRecord wsolr = this.innerMap(key, value, reporter);

        // Pass to reduce stage if successful:
        if (wsolr != null) {

            // Use a random assignment per shard:
            // int iKey = (int) (Math.round(Math.random() * numShards));
            // Use a random assignment per reducer:
            int iKey = (int) (Math.round(Math.random() * opts.num_reducers));

            IntWritable oKey = new IntWritable(iKey);
            output.collect(oKey, wsolr);

        }
    }


}
