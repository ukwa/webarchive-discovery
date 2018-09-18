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

import java.io.*;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    
    private static Log log = LogFactory.getLog(WARCIndexerCommand.class);
    static {
        Instrument.init();
    }

    private static final String CLI_USAGE = "[-o <output dir>] [-s <Solr instance>] [-t] <include text> [-r] <root/slash pages only> [-b <batch-submissions size>] [WARC File List]";
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
        String outputDir = null;
        String solrUrl = null;
        String configFile = null;
        boolean isTextRequired = false;
        boolean slashPages = false;
        int batchSize = -1; // No explicit batch size (defaults to 1 if not stated in the conf-file)
        String annotationsFile = null;
        boolean disableCommit;
        
        Options options = new Options();
        options.addOption("o", "output", true,
                "The directory to contain the output XML files");
        options.addOption("z", "gzip", false,
                "Pack the output XML files in a single gzipped XML file (only valid when -o has been specified)");
        options.addOption("s", "solr", true,
                "The URL of the Solr instance the document should be sent to");
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

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
               String cli_args[] = line.getArgs();
           
        
            // Check that a mandatory Archive file(s) has been supplied
            if( !( cli_args.length > 0 ) ) {
                printUsage( options );
                System.exit( 0 );
            }

            boolean gzip = line.hasOption("z");

               // Get the output directory, if set
               if(line.hasOption("o")){
                   outputDir = line.getOptionValue("o");
                   if(outputDir.endsWith("/")||outputDir.endsWith("\\")){
                       outputDir = outputDir.substring(0, outputDir.length()-1);
                   }
        
                   outputDir = outputDir + "//";
                   System.out.println("Output Directory is: " + outputDir + " with gzip=" + gzip);
                   File dir = new File(outputDir);
                   if(!dir.exists()){
                       FileUtils.forceMkdir(dir);
                   }
               }

               // Get the Solr Url, if set
               if(line.hasOption("s")){
                   solrUrl = line.getOptionValue("s");
                   if(solrUrl.contains("\"")){
                       solrUrl  = solrUrl.replaceAll("\"", "");
                }
               }
               
               // Check if the text field is required in the XML output
               if(line.hasOption("t") || line.hasOption("s")){
                   isTextRequired = true;
               }

               if( line.hasOption( "r" ) ) {
                   slashPages = true;
               }

               if( line.hasOption( "b" ) ) {
                   batchSize = Integer.parseInt( line.getOptionValue( "b" ) );
               }
               
            if (line.hasOption("c")) {
                configFile = line.getOptionValue("c");
            }

               // Check that either an output dir or Solr URL is supplied
               if(outputDir == null && solrUrl == null){
                   System.out.println( "A Solr URL or an Output Directory must be supplied" );
                   printUsage(options);
                   System.exit( 0 );
               }
               
               // Check that both an output dir and Solr URL are not supplied
               if(outputDir != null && solrUrl != null){
                   System.out.println( "A Solr URL and an Output Directory cannot both be specified" );
                   printUsage(options);
                   System.exit( 0 );
               }

            // Pick up any annotations specified:
            if (line.hasOption("a")) {
                annotationsFile = line.getOptionValue("a");
            }
            
            if (line.hasOption("i")) {
              institution = line.getOptionValue("i");
            }
            
            if (line.hasOption("n")) {
              collection = line.getOptionValue("n");
            }
            
            if (line.hasOption("u")) {
              collection_id = line.getOptionValue("u");
            }

            // Check for commit disabling
            disableCommit = line.hasOption("d");

            parseWarcFiles(configFile, outputDir, gzip, solrUrl, cli_args,
                           isTextRequired, slashPages, batchSize, annotationsFile,
                           disableCommit, institution, collection, collection_id);
        
        } catch (org.apache.commons.cli.ParseException e) {
            log.error("Parse exception when processing command line arguments: "+e);
        } finally {
            Instrument.timeRel("WARCIndexerCommand.main#total", allStart);
            Instrument.log(true);
        }
    }
    
    /**
     * @param outputDir
     * @param args
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws TransformerFactoryConfigurationError
     * @throws TransformerException
     */
    public static void parseWarcFiles(String configFile, String outputDir, boolean gzip,
            String solrUrl, String[] args, boolean isTextRequired,
            boolean slashPages, int batchSize, String annotationsFile,
            boolean disableCommit, String institution, String collection,
            String collection_id)
            throws NoSuchAlgorithmException,
            TransformerFactoryConfigurationError, TransformerException,
            IOException {
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
        if(solrUrl != null) {
            conf = conf.withValue(SolrWebServer.CONF_HTTP_SERVER, ConfigValueFactory.fromAnyRef(solrUrl) );
        }
        // Use config for default value
        if (conf.hasPath("warc.solr.disablecommit")) {
            disableCommit = disableCommit || conf.getBoolean("warc.solr.disablecommit");
        }

        if (batchSize == -1) { // Batch size not set as command line, so resolve it from conf with default 1
            batchSize = conf.hasPath("warc.solr.batch_size") ? conf.getInt("warc.solr.batch_size") : 1;
        }

        // Set up the server config:
        SolrWebServer solrWeb = new SolrWebServer(conf);
        

        // Also pass config down:
        WARCIndexer windex = new WARCIndexer(conf);

        // Add in annotations, if set:
        if (annotationsFile != null) {
            Annotations ann = Annotations.fromJsonFile(annotationsFile);
            SurtPrefixSet oaSurts = Annotator
                    .loadSurtPrefix("openAccessSurts.txt");
            windex.setAnnotations(ann, oaSurts);
        }
        

        // To be indexed:
        ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(); 
        int totInputFile = args.length;
        int curInputFile = 1;
                    
        Instrument.timeRel("WARCIndexerCommand.main#total",
                           "WARCIndexerCommand.parseWarcFiles#startup", start);
        // Loop through each Warc files
        for (int arcsIndex = 0; arcsIndex < args.length; arcsIndex++) {
            final long arcStart = System.nanoTime();
            String inputFile = args[arcsIndex];
            if (!disableCommit) {
                // Commit to make sure index is up to date:
                commit(solrWeb);
            }

            System.out.println("Parsing Archive File [" + curInputFile + "/" + totInputFile + "]:" + inputFile);
            File inFile = new File(inputFile);
            String fileName = inFile.getName();
            String outputWarcDir = outputDir + fileName + "//";
            Writer zipOut = outputDir == null || !gzip ? null :
                    new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(
                            outputDir + fileName + ".xml.gz"))), Charset.forName("utf-8"));
            if (zipOut != null) {
                zipOut.write("<add>");
            }

            File dir = new File(outputWarcDir);
            if (!dir.exists() && solrUrl == null && zipOut == null) {
                FileUtils.forceMkdir(dir);
            }

            ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
            Iterator<ArchiveRecord> ir = reader.iterator();
            int recordCount = 1;
            int lastFailedRecord = 0;

            // Iterate though each record in the WARC file
            while (ir.hasNext()) {
                final long recordStart = System.nanoTime();
                ArchiveRecord rec = null;
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
                    final long updateStart = System.nanoTime();
                    File fileOutput = new File(outputWarcDir + "//" + "FILE_" + recordCount + ".xml");

                    if (!slashPages || (doc.getFieldValue(SolrFields.SOLR_URL_TYPE) != null &&
                                        doc.getFieldValue(SolrFields.SOLR_URL_TYPE).equals(SolrFields.SOLR_URL_TYPE_SLASHPAGE))) {

                        if (zipOut != null) {
                            doc.writeXml(zipOut);
                        } else if (solrUrl == null) {
                            writeXMLToFile(doc.toXml(), fileOutput);
                        } else {
                            docs.add(doc.getSolrDocument());
                            checkSubmission(solrWeb, docs, batchSize, false);
                        }
                        recordCount++;
                    }
                    Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#fullarcprocess",
                                       "WARCIndexerCommand.parseWarcFiles#docdelivery", updateStart);
                }
            }
            curInputFile++;
            if (zipOut != null) {
                zipOut.write("</add>");
                zipOut.flush();
                zipOut.close();
            }
            Instrument.timeRel("WARCIndexerCommand.main#total",
                               "WARCIndexerCommand.parseWarcFiles#fullarcprocess", arcStart);
            Instrument.log(arcsIndex < args.length-1); // Don't log the last on info to avoid near-duplicate logging
        }

        // Submit any remaining docs:
        checkSubmission(solrWeb, docs, batchSize, true);
        if (!disableCommit) {
            // Commit the updates:
            commit(solrWeb);
        }

        long endTime = System.currentTimeMillis();

        System.out.println("WARC Indexer Finished in " + ((endTime - startTime) / 1000.0) + " seconds.");
    }
    
    private static void commit( SolrWebServer solrWeb) {
        // Commit any Solr Updates
        if( solrWeb != null ) {
            try {
                final long start = System.nanoTime();
                solrWeb.commit();
                Instrument.timeRel("WARCIndexerCommand.main#total", "WARCIndexerCommand.commit#success", start);
            } catch( SolrServerException s ) {
                log.warn( "SolrServerException when committing.", s );
            } catch( IOException i ) {
                log.warn( "IOException when committing.", i );
            }
        }
    }

    /**
     * Checks whether a List of SolrInputDocuments has grown large enough to
     * be submitted to a SolrWebServer.
     * 
     * @param solr
     * @param docs
     * @param limit
     * @throws SolrServerException
     * @throws IOException
     */
    private static void checkSubmission(SolrWebServer solr,
            List<SolrInputDocument> docs, int limit, boolean force) {
        if (docs.size() > 0 && (docs.size() >= limit || force)) {
            try {
                final long start = System.nanoTime();
                if (log.isTraceEnabled() || debugMode) {
                    for (SolrInputDocument doc : docs) {
                        try {
                            solr.updateSolrDoc(doc);
                        } catch (Exception e) {
                            log.error(
                                    "Failed to post document - got exception: ",
                                    e);
                            log.error("Failed document was:\n"
                                    + ClientUtils.toXML(doc));
                            System.exit(1);
                        }
                    }
                } else {
                    solr.add(docs);
                }
                Instrument.timeRel(
                        "WARCIndexerCommand.parseWarcFiles#docdelivery",
                        "WARCIndexerCommanc.checkSubmission#solrSendBatch", start);
                docs.clear();
            } catch (SolrServerException s) {
                log.warn("SolrServerException: ", s);
            } catch (IOException i) {
                log.warn("IOException: ", i);
            }

        }
    }
    
    
    public static void prettyPrintXML( String doc ) throws TransformerFactoryConfigurationError, TransformerException {
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        //initialize StreamResult with File object to save to file
        StreamResult result = new StreamResult(new StringWriter());
        StreamSource source = new StreamSource(new StringReader(doc));
        transformer.transform(source, result);
        String xmlString = result.getWriter().toString();
        System.out.println(xmlString);        
    }
    
    /**
     * @param xml
     * @param file
     * @throws IOException
     * @throws TransformerFactoryConfigurationError
     * @throws TransformerException
     */
    public static void writeXMLToFile( String xml, File file ) throws IOException, TransformerFactoryConfigurationError, TransformerException {
        
        Result result = new StreamResult(file);
        Source source =  new StreamSource(new StringReader(xml));
        
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        //FileUtils.writeStringToFile(file, xml);
        
        transformer.transform(source, result);
      
    }
    
    /**
     * @param options
     */
    private static void printUsage( Options options ) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( CLI_USAGE, CLI_HEADER, options, CLI_FOOTER );
    }
    
}
