/**
 * 
 */
package uk.bl.wa.hadoop.indexer;

/*-
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

import java.io.File;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import uk.bl.wa.solr.SolrWebServer.SolrOptions;

/**
 * 
 * Declaration of WARC Indexer options, using picocli.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@Command(name = "WARCIndexer", description = "Options for the WARCIndexer", mixinStandardHelpOptions = true)
public class WARCIndexerOptions {

    @Option(names = { "-R",
            "--num-reducers" }, description = "Number of reducer tasks to use. Default is ${DEFAULT-VALUE}.", defaultValue = "5")
    public int num_reducers;

    @Option(names = { "-d",
            "--dump" }, description = "Dump configuration and exit.", defaultValue = "false")
    public boolean dump;

    @Option(names = { "-x",
            "--xml" }, description = "Output XML in OAI-PMH format.", defaultValue = "false")
    public boolean xml;

    @Option(names = { "-w",
            "--wait" }, description = "Wait for job to finish.", defaultValue = "false")
    public boolean wait;

    @Option(names = { "-a",
            "--annotations" }, description = "apply annotations from fixed-name files, via '-files annotations.json,openAccessSurts.txt'", defaultValue = "false")
    public boolean annotations;

    @Option(names = "-c", description = "Local file that contains the indexer configuration", required = false)
    public File config;

    @Option(names = "-i", description = "Local input file that contains a list of HDFS paths to WARCs to be indexed.", required = true)
    public File input;

    @Option(names = "-o", description = "HDFS folder the job should write to.", required = true)
    public String output;

    @Option(names = "--dummy-run", description = "Run the indexing process but do not attempt to push data to Solr.", defaultValue = "false")
    public boolean dummyRun;

    // Add in Solr options:
    @Mixin
    public SolrOptions solr;

}
