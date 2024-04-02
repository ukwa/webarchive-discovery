/**
 * 
 */
package uk.bl.wa.indexer;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2023 The webarchive-discovery project contributors
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

import java.io.*;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.util.SurtPrefixSet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import picocli.CommandLine;
import picocli.CommandLine.ParseResult;
import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.annotation.Annotator;
import uk.bl.wa.indexer.WARCIndexerCommandOptions.OutputFormat;
import uk.bl.wa.indexer.delivery.DocumentConsumer;
import uk.bl.wa.indexer.delivery.DocumentConsumerFactory;
import uk.bl.wa.opensearch.OpensearchImporter;
import uk.bl.wa.opensearch.OpensearchUrl;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexerCommand {
    
    private static Logger log = LoggerFactory.getLogger(WARCIndexerCommand.class);
    static {
        Instrument.init();
    }

    static WARCIndexerCommandOptions opts = new WARCIndexerCommandOptions();


    /**
     * 
     * @param args
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws TransformerException 
     * @throws TransformerFactoryConfigurationError 
     * @throws SolrServerException 
     */
    public static void main( String[] args ) throws NoSuchAlgorithmException, IOException, TransformerFactoryConfigurationError, TransformerException {
        final long allStart = System.nanoTime();

        CommandLine cli = new CommandLine(opts);
        ParseResult pr = cli.parseArgs(args);
        if (pr.isUsageHelpRequested()) {
            cli.usage(System.out);
            return;
        }

        // Check that either an output dir or Solr URL or Opensearch URL is supplied
        if(opts.output == null && opts.solrUrl == null && opts.opensearchUrl == null){
            System.out.println( "A Solr URL, Opensearch URL or an Output Directory must be supplied" );
            cli.usage(System.out);
            System.exit( 0 );
        }
        // Check that both an output dir and Solr/Opensearch URL are not supplied
        if((opts.output != null && opts.solrUrl != null) || (opts.output != null && opts.opensearchUrl != null)){
            System.out.println( "A Solr/Opensearch URL and an Output Directory cannot both be specified" );
            cli.usage(System.out);
            System.exit( 0 );
        }

        // Check if the text field is required for the output (explicit (-t), Elasticsearch or Solr)
        final boolean isTextRequired = opts.includeText || opts.solrUrl != null || opts.opensearchUrl != null;

        try {
            parseWarcFiles(opts, isTextRequired);
        } finally {
            Instrument.timeRel("WARCIndexerCommand.main#total", allStart);
            Instrument.log(true);
        }
    }

    /**
     * @param outputDir
     * @param inputFiles
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws TransformerFactoryConfigurationError
     * @throws TransformerException
     */
    public static void parseWarcFiles( WARCIndexerCommandOptions opts, boolean isTextRequired )
            throws NoSuchAlgorithmException, TransformerFactoryConfigurationError, IOException {
        long startTime = System.currentTimeMillis();
        final long start = System.nanoTime();

        // Setup default config:
        Config conf = ConfigFactory.load();
        if( opts.output != null && opts.outputFormat.equals(OutputFormat.jsonl)) {
            log.warn("As generating JSONL, using the dataset-generation default configuration...");
            conf = ConfigFactory.load("dataset-generation");
        }
        // Allow config override:
        if (opts.config != null) {
            log.info("Loading config from log file: " + opts.config);
            File configFilePath = new File(opts.config);
            if (!configFilePath.exists()){
              log.error("Config file not found:"+opts.config);
              System.exit( 0 );                          
            }
            
            conf = ConfigFactory.parseFile(configFilePath);
        }

        // FIXME DUMP CONFIG OPTIONS
        // ConfigPrinter.print(conf);
        // conf.withOnlyPath("warc").root().render(ConfigRenderOptions.concise()));
        log.info("Loaded indexer config: " + conf.getString("warc.title"));

        final SolrRecordFactory solrFactory = SolrRecordFactory.createFactory(conf);
        final DocumentConsumer docConsumer = DocumentConsumerFactory.createConsumer(
                conf, opts.output, opts.outputFormat, opts.compressOutput, opts.solrUrl, 
                opts.opensearchUrl, opts.user, opts.password, opts.batchSize, null, opts.disableCommit);

        // Also pass config down:
        WARCIndexer windex = new WARCIndexer(conf);

        // Add in annotations, if set:
        if (opts.annotations != null) {
            Annotations ann = Annotations.fromJsonFile(opts.annotations);
            SurtPrefixSet oaSurts = Annotator.loadSurtPrefix("openAccessSurts.txt");
            windex.setAnnotations(ann, oaSurts);
        }

        Instrument.timeRel("WARCIndexerCommand.main#total",
                           "WARCIndexerCommand.parseWarcFiles#startup", start);

        // Loop through each Warc files
        for (int arcsIndex = 0; arcsIndex < opts.inputFiles.length; arcsIndex++) {
            final long arcStart = System.nanoTime();
            String inputFile = opts.inputFiles[arcsIndex];

            System.out.println("Parsing Archive File [" + (arcsIndex+1) + "/" + opts.inputFiles.length + "]:" + inputFile);
            docConsumer.startWARC(inputFile);
            File inFile = new File(inputFile);

            ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
            Iterator<ArchiveRecord> ir = reader.iterator();
            int recordCount = 1;
            int lastFailedRecord = 0;

            // Iterate though each record in the WARC file
            while (ir.hasNext()) {
                final long recordStart = System.nanoTime();
                ArchiveRecord rec;
                try {
                    rec = ir.next();
                } catch (RuntimeException e) {
                    log.warn("Exception on record after rec " + recordCount + " from " + inFile.getName(), e);
                    if (lastFailedRecord != recordCount) {
                        lastFailedRecord = recordCount;
                        continue;
                    }
                    log.error("Failed to reach next record, last record already on error - skipping the rest of the records");
                    break;
                }
                final String url = Normalisation.sanitiseWARCHeaderValue(rec.getHeader().getUrl());
                final String type = (String)rec.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE);
                SolrRecord doc = solrFactory.createRecord(inFile.getName(), rec.getHeader());
                log.debug("Processing record " + type + " for url " + url + " from " + inFile.getName() + " @"
                        + rec.getHeader().getOffset());
                try {
                    doc = windex.extract(inFile.getName(), rec, isTextRequired);
                } catch (Exception e) {
                    log.warn("Exception on record " + url + " from " + inFile.getName(), e);
                    doc.addParseException(e);
                    continue;
                } catch (OutOfMemoryError e) {
                    log.warn("OutOfMemoryError on record " + url + " from " + inFile.getName(), e);
                    doc.addParseException(e);
                }

                Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#fullarcprocess",
                                   "WARCIndexerCommand.parseWarcFiles#solrdocCreation", recordStart);
                if (doc != null) {
                    if (!opts.onlyRootPages || (doc.getFieldValue(SolrFields.SOLR_URL_TYPE) != null &&
                                        doc.getFieldValue(SolrFields.SOLR_URL_TYPE).equals(SolrFields.SOLR_URL_TYPE_SLASHPAGE))) {
                        docConsumer.add(doc);
                        recordCount++;
                    }
                } else {
                    log.debug("No document produced by record: " + type + " for url " + url + " from " + 
                        inFile.getName() + " @" + rec.getHeader().getOffset()); //All request records will log this. It is expected there is no document.
            }
            }
            docConsumer.endWARC();
            Instrument.timeRel("WARCIndexerCommand.main#total",
                               "WARCIndexerCommand.parseWarcFiles#fullarcprocess", arcStart);
            Instrument.log(arcsIndex < opts.inputFiles.length-1); // Don't log the last on info to avoid near-duplicate logging
        }

        // Submit any remaining docs:
        docConsumer.close();

        long endTime = System.currentTimeMillis();
        System.out.println("WARC Indexer Finished in " + ((endTime - startTime) / 1000.0) + " seconds.");
    }
    
}
