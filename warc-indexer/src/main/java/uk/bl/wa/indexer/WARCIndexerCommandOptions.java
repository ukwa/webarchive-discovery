package uk.bl.wa.indexer;

/*-
 * #%L
 * warc-hadoop-indexer
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

import java.io.File;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import uk.bl.wa.solr.SolrWebServer.SolrOptions;

/**
 * 
 * Declaration of WARC Indexer CLI options, using picocli.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@Command(name = "WARCIndexer", description = "Extracts metadata and text from Archive Records", mixinStandardHelpOptions = true, separator = " ")
public class WARCIndexerCommandOptions {

    /* 
     * 
   "[-o <output dir>] 
    [-s <Solr instance>] 
    [-e <Opensearch instance>] 
    [-u <user>] 
    [-p <password>] 
    [-t] <include text> 
    [-r] <root/slash pages only> 
    [-b <batch-submissions size>] 
    [WARC File List]";

    private static Options getParserOptions() {
        Options options = new Options();
        return options;
    }    
    */


    @Option(names = { "--dump" }, description = "Dump configuration and exit.", defaultValue = "false")
    public boolean dump;

    @Option(names = { "-a",
            "--annotations" }, description = "A JSON file containing the annotations to apply during indexing.'")
    public String annotations;

    @Option(names = { "-i", "--institution" }, description = "Institution.")
    public String institution;

    @Option(names = { "-n", "--collection" }, description = "Collection.")
    public String collection;

    @Option(names = { "-u", "--collection-id" }, description = "Collection ID.")
    public String collectionId;

    @Option(names = { "-t", "--text"}, description = "Include text in output files.", defaultValue = "true")
    public boolean includeText;

    @Option(names = { "-r", "--slash"}, description = "Only process slash (root) pages.", defaultValue = "false")
    public boolean onlyRootPages;

    @Option(names = { "-c", "--config" }, description = "Local file that contains the indexer configuration.")
    public String config;

    //@Option(names = "--no-solr", description = "Run the indexing process but do not attempt to push data to Solr.", defaultValue = "false")
    //public boolean dummyRun;

    @Option(names = { "-o", "--output" }, description = "The directory to contain the output files.")
    public String output;

    public enum OutputFormat { jsonl, xml }
    @Option(names = {"-F", "--output-format"}, description = "Which format, 'jsonl' or 'xml', to use when output is specified (rather than direct indexing). Default is 'xml'.", defaultValue = "xml")
    public OutputFormat outputFormat;

    @Option(names = { "-z", "--gzip", "--compress" }, description = "Compress the final output file(s).", defaultValue = "false")
    public boolean compressOutput;

    // Add in Solr options? More complex case here:
    //@Mixin
    //public SolrOptions solr;

    @Option(names = { "-s", "-S", "--solr-endpoint" }, description = "The HTTP Solr endpoint to use. ")
    public String solrUrl;

    @Option(names = { "-e", "--opensearch" }, description = "The URL of the Opensearch instance the document should be sent to.")
    public String opensearchUrl;
    
    @Option(names = { "-user", "--user" }, description = "The user for the fulltext instance.")
    public String user;

    @Option(names = { "-password", "--password" }, description = "The userpassword for the fulltext instance.")
    public String password;

    @Option(names = { "-b", "--batch" }, description = "Batch size for submissions.", defaultValue = "100")
    public Integer batchSize;

    @Option(names = { "-d", "--disable_commit", "--disable-commit" }, description = "Disable client side commits (speeds up indexing at the cost of flush guarantee).")
    public Boolean disableCommit;

    // The input files:
    @Parameters(paramLabel = "FILE", description = "Input file that contains a list of WARCs to be processed.")
    public String[] inputFiles;

}
