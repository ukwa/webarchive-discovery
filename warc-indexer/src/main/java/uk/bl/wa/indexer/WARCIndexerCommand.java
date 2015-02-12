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
 * Copyright (C) 2013 - 2014 The UK Web Archive
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

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;

import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import uk.bl.wa.util.Instrument;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexerCommand {
	
	private static Log log = LogFactory.getLog(WARCIndexerCommand.class);
	

	private static final String CLI_USAGE = "[-o <output dir>] [-s <Solr instance>] [-t] <include text> [-r] <root/slash pages only> [-b <batch-submissions size>] [WARC File List]";
	private static final String CLI_HEADER = "WARCIndexer - Extracts metadata and text from Archive Records";
	private static final String CLI_FOOTER = "";
	
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
		int batchSize = 1;
		String annotationsFile = null;
        boolean disableCommit;
		
		Options options = new Options();
		options.addOption("o", "output", true,
				"The directory to contain the output XML files");
		options.addOption("s", "solr", true,
				"The URL of the required Solr Instance");
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

		try {
		    // parse the command line arguments
		    CommandLine line = parser.parse( options, args );
		   	String cli_args[] = line.getArgs();
		   
		
			// Check that a mandatory Archive file(s) has been supplied
			if( !( cli_args.length > 0 ) ) {
				printUsage( options );
				System.exit( 0 );
			}

		   	// Get the output directory, if set
		   	if(line.hasOption("o")){
		   		outputDir = line.getOptionValue("o");
		   		if(outputDir.endsWith("/")||outputDir.endsWith("\\")){
		   			outputDir = outputDir.substring(0, outputDir.length()-1);
		   		}
		
		   		outputDir = outputDir + "//";
		   		System.out.println("Output Directory is: " + outputDir);
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

            // Check for commit disabling
            disableCommit = line.hasOption("d");

			parseWarcFiles(configFile, outputDir, solrUrl, cli_args,
                           isTextRequired, slashPages, batchSize, annotationsFile, disableCommit);
		
		} catch (org.apache.commons.cli.ParseException e) {
			log.error("Parse exception when processing command line arguments: "+e);
		}
        Instrument.timeRel("WARCIndexerCommand.all", allStart);
	}
	
	/**
	 * @param outputDir
	 * @param args
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 */
	public static void parseWarcFiles(String configFile, String outputDir,
			String solrUrl, String[] args, boolean isTextRequired,
			boolean slashPages, int batchSize, String annotationsFile,
            boolean disableCommit)
			throws NoSuchAlgorithmException,
			TransformerFactoryConfigurationError, TransformerException,
			IOException {
		long startTime = System.currentTimeMillis();

		// If the Solr URL is set initiate a connections
		Config conf = ConfigFactory.load();
		if (configFile != null) {
			log.info("Loading config from log file: " + configFile);
			conf = ConfigFactory.parseFile(new File(configFile));
			// ConfigPrinter.print(conf);
			// conf.withOnlyPath("warc").root().render(ConfigRenderOptions.concise()));
			log.info("Loaded warc config.");
			log.info(conf.getString("warc.title"));
		}
		if(solrUrl != null) {
			conf = conf.withValue(SolrWebServer.CONF_HTTP_SERVER, ConfigValueFactory.fromAnyRef(solrUrl) );
		}
        // Use config for default value
        if (conf.hasPath("warc.solr.disablecommit")) {
            disableCommit = disableCommit || conf.getBoolean("warc.solr.disablecommit");
        }
		
		// Set up the server config:
		SolrWebServer solrWeb = new SolrWebServer(conf);
		

		// Also pass config down:
		WARCIndexer windex = new WARCIndexer(conf);

		// Add in annotations, if set:
		if (annotationsFile != null) {
			Annotations ann = Annotations.fromJsonFile(annotationsFile);
			windex.setAnnotations(ann);
		}
		
		// To be indexed:
		ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(); 
		int totInputFile = args.length;
		int curInputFile = 1;
					
		// Loop through each Warc files
		for( String inputFile : args ) {
            if (!disableCommit) {
                // Commit to make sure index is up to date:
                commit(solrWeb);
            }

			System.out.println("Parsing Archive File [" + curInputFile + "/" + totInputFile + "]:" + inputFile);
			File inFile = new File(inputFile);
			String fileName = inFile.getName();
			String outputWarcDir = outputDir + fileName + "//";
			File dir = new File(outputWarcDir);
	   		if(!dir.exists() && solrUrl == null){
	   			FileUtils.forceMkdir(dir);
	   		}
			
			ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
			Iterator<ArchiveRecord> ir = reader.iterator();
			int recordCount = 1;
			
			// Iterate though each record in the WARC file
			while( ir.hasNext() ) {
                final long recordStart = System.nanoTime();
				ArchiveRecord rec = ir.next();
				SolrRecord doc = new SolrRecord(inFile.getName(),
						rec.getHeader());
				try {
					doc = windex.extract(inFile.getName(), rec, isTextRequired);
				} catch (Exception e) {
					log.warn("Exception on record "+rec.getHeader().getUrl()+ " from "+inFile.getName(), e);
					doc.addParseException(e);
					continue;
				} catch (OutOfMemoryError e) {
					log.warn(
							"OutOfMemoryError on record "
									+ rec.getHeader().getUrl() + " from "
									+ inFile.getName(), e);
					doc.addParseException(e);
				}

				if( doc != null ) {
                    Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#solrdocCreation", recordStart);
					File fileOutput = new File(outputWarcDir + "//" + "FILE_" + recordCount + ".xml");
					
					if( !slashPages || ( doc.getFieldValue( SolrFields.SOLR_URL_TYPE ) != null &&
									     doc.getFieldValue( SolrFields.SOLR_URL_TYPE ).equals( SolrFields.SOLR_URL_TYPE_SLASHPAGE ) ) ) {
						// Write XML to file if not posting straight to the server.
						if(solrUrl == null) {
							writeXMLToFile(doc.toXml(), fileOutput);
						}else{
							// Post to Solr
							try {
								docs.add( doc.getSolrDocument() );
								checkSubmission( solrWeb, docs, batchSize );
							} catch( SolrServerException s ) {
								log.warn( "SolrServerException: " + inputFile, s );
							} catch( IOException i ) {
								log.warn( "IOException: " + inputFile, i );
							}
						}
						recordCount++;
					}

				}			
			}
			curInputFile++;
            Instrument.log(false);
		}

        if (!disableCommit) {
            // Commit the updates:
            commit(solrWeb);
        }

		long endTime = System.currentTimeMillis();

		System.out.println("WARC Indexer Finished in "+((endTime-startTime)/1000.0)+" seconds.");
        Instrument.log(true);
	}
	
	private static void commit( SolrWebServer solrWeb) {
		// Commit any Solr Updates
		if( solrWeb != null ) {
			try {
                final long start = System.nanoTime();
				solrWeb.commit();
                Instrument.timeRel("WARCIndexerCommand.commit#success", start);
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
	private static void checkSubmission( SolrWebServer solr, List<SolrInputDocument> docs, int limit ) throws SolrServerException, IOException {
		if( docs.size() > 0 && docs.size() >= limit ) {
			solr.add( docs );
            docs.clear();
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
