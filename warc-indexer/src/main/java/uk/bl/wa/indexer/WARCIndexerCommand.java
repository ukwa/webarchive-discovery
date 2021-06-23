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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.util.SurtPrefixSet;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.annotation.Annotator;
import uk.bl.wa.elastic.ElasticImporter;
import uk.bl.wa.elastic.ElasticUrl;
import uk.bl.wa.indexer.delivery.DocumentConsumer;
import uk.bl.wa.indexer.delivery.DocumentConsumerFactory;
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

    private static final String CLI_USAGE = "[-o <output dir>] [-s <Solr instance>] [-e <Elastic instance>] [-t] <include text> [-r] <root/slash pages only> [-b <batch-submissions size>] [WARC File List]";
    private static final String CLI_HEADER = "WARCIndexer - Extracts metadata and text from Archive Records";
    private static final String CLI_FOOTER = "";
    
    private static boolean debugMode = false;
    
    public static String institution;
    public static String collection;
    public static String collection_id;

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
        CommandLineParser parser = new PosixParser();
        final Options options = getParserOptions();

        CommandLine line;
        try {
            line = parser.parse(options, args );
        } catch (org.apache.commons.cli.ParseException e) {
            log.error("Parse exception when processing command line arguments: " + e);
            printUsage(options);
            throw new IllegalArgumentException("Exception parsing arguments", e);
        }

        final String[] inputFiles = line.getArgs();
        // Check that a mandatory Archive file(s) has been supplied
        if (inputFiles.length == 0) {
            printUsage( options );
            System.exit( 0 );
        }

        // Either file output, solr or elastic are required
        final String outputDir = line.hasOption("o") ? line.getOptionValue("o") : null;
        final String solrUrl = line.hasOption("s") ? line.getOptionValue("s").replaceAll("\"", "") : null;
        final String elasticUrl = line.hasOption("e") ? line.getOptionValue("e") : null;
        boolean gzip = line.hasOption("z"); // Used with outputDir
        final Boolean disableCommit = line.hasOption("d") ? true : null; // Used with Solr (and possibly Elasticsearch?)
        // -1 means no explicit batch size (defaults to 1 if not stated in the conf-file)
        final Integer batchSize = line.hasOption("b") ? Integer.parseInt(line.getOptionValue("b")) : null;

        // Check that either an output dir or Solr URL or Elastic URL is supplied
        if(outputDir == null && solrUrl == null && elasticUrl == null){
            System.out.println( "A Solr URL, Elastic URL or an Output Directory must be supplied" );
            printUsage(options);
            System.exit( 0 );
        }
        // Check that both an output dir and Solr/Elastic URL are not supplied
        if((outputDir != null && solrUrl != null) || (outputDir != null && elasticUrl != null)){
            System.out.println( "A Solr/Elastic URL and an Output Directory cannot both be specified" );
            printUsage(options);
            System.exit( 0 );
        }

        // Check if the text field is required in the XML output
        final boolean isTextRequired = line.hasOption("t") || line.hasOption("s");

        final boolean slashPages = line.hasOption("r");

        final String configFile = line.hasOption("c") ? line.getOptionValue("c") : null;

        // Pick up any annotations specified:
        final String annotationsFile = line.hasOption("a") ? line.getOptionValue("a") : null;

        institution = line.hasOption("i") ? line.getOptionValue("i") : null;
        collection = line.hasOption("n") ? line.getOptionValue("n") : null;
        collection_id = line.hasOption("u") ? line.getOptionValue("u") : null;

        try {
            parseWarcFiles(configFile, outputDir, gzip, solrUrl, elasticUrl, inputFiles,
                           isTextRequired, slashPages, batchSize, annotationsFile,
                           disableCommit, institution, collection, collection_id);
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
    public static void parseWarcFiles(
            String configFile,
            String outputDir, boolean gzip, String solrUrl, String elasticUrl,
            String[] inputFiles,
            boolean isTextRequired, boolean slashPages,
            Integer batchSize, String annotationsFile, Boolean disableCommit,
            String institution, String collection, String collection_id)
            throws NoSuchAlgorithmException, TransformerFactoryConfigurationError, IOException {
        long startTime = System.currentTimeMillis();
        final long start = System.nanoTime();

        // If the Solr URL is set initiate a connections
        Config conf = ConfigFactory.load();
        if (configFile != null) {
            log.info("Loading config from log file: " + configFile);
            File configFilePath = new File(configFile);
            if (!configFilePath.exists()){
              log.error("Config file not found:"+configFile);
              System.exit( 0 );                          
            }
            
            conf = ConfigFactory.parseFile(configFilePath);
            // ConfigPrinter.print(conf);
            // conf.withOnlyPath("warc").root().render(ConfigRenderOptions.concise()));
            log.info("Loaded warc config.");
            log.info(conf.getString("warc.title"));
        }
        final SolrRecordFactory solrFactory = SolrRecordFactory.createFactory(conf);
        final DocumentConsumer docConsumer = DocumentConsumerFactory.createConsumer(
                conf, outputDir, gzip, solrUrl, elasticUrl, batchSize, null, disableCommit);

        // Also pass config down:
        WARCIndexer windex = new WARCIndexer(conf);

        // Add in annotations, if set:
        if (annotationsFile != null) {
            Annotations ann = Annotations.fromJsonFile(annotationsFile);
            SurtPrefixSet oaSurts = Annotator.loadSurtPrefix("openAccessSurts.txt");
            windex.setAnnotations(ann, oaSurts);
        }

        Instrument.timeRel("WARCIndexerCommand.main#total",
                           "WARCIndexerCommand.parseWarcFiles#startup", start);

        // Loop through each Warc files
        for (int arcsIndex = 0; arcsIndex < inputFiles.length; arcsIndex++) {
            final long arcStart = System.nanoTime();
            String inputFile = inputFiles[arcsIndex];

            System.out.println("Parsing Archive File [" + (arcsIndex+1) + "/" + inputFiles.length + "]:" + inputFile);
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
                SolrRecord doc = solrFactory.createRecord(inFile.getName(), rec.getHeader());
                log.debug("Processing record for url " + url + " from " + inFile.getName() + " @"
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
                    if (!slashPages || (doc.getFieldValue(SolrFields.SOLR_URL_TYPE) != null &&
                                        doc.getFieldValue(SolrFields.SOLR_URL_TYPE).equals(SolrFields.SOLR_URL_TYPE_SLASHPAGE))) {
                        docConsumer.add(doc);
                        recordCount++;
                    }
                }
            }
            docConsumer.endWARC();
            Instrument.timeRel("WARCIndexerCommand.main#total",
                               "WARCIndexerCommand.parseWarcFiles#fullarcprocess", arcStart);
            Instrument.log(arcsIndex < inputFiles.length-1); // Don't log the last on info to avoid near-duplicate logging
        }

        // Submit any remaining docs:
        docConsumer.close();

        long endTime = System.currentTimeMillis();
        System.out.println("WARC Indexer Finished in " + ((endTime - startTime) / 1000.0) + " seconds.");
    }
    
    /**
     * @param options the options to print to stdout.
     */
    private static void printUsage( Options options ) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( CLI_USAGE, CLI_HEADER, options, CLI_FOOTER );
    }
    
    @NotNull
    private static Options getParserOptions() {
        Options options = new Options();
        options.addOption("o", "output", true,
                          "The directory to contain the output XML files");
        options.addOption("z", "gzip", false,
                          "Pack the output XML files in a single gzipped XML file (only valid when -o has been specified)");
        options.addOption("s", "solr", true,
                          "The URL of the Solr instance the document should be sent to");
        options.addOption("e", "elastic", true,
                          "The URL of the Elastic instance the document should be sent to");
        options.addOption("t", "text", false,
                          "Include text in XML in output files");
        options.addOption("r", "slash", false,
                          "Only process slash (root) pages.");
        options.addOption("a", "annotations", true,
                          "A JSON file containing the annotations to apply during indexing.");
        options.addOption("b", "batch", true, "Batch size for submissions.");
        options.addOption("c", "config", true, "Configuration to use.");
        options.addOption("d", "disable_commit", false,
                          "Disable client side commits (speeds up indexing at the cost of flush guarantee).");
        options.addOption("i", "institution", true, "Institution.");
        options.addOption("n", "collection", true, "Collection.");
        options.addOption("u", "collection_id", true, "Collection ID.");
        return options;
    }

}
