/**
 * 
 */
package uk.bl.wa.hadoop.indexer;

import java.io.File;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Declaration of WARC Hadoop Indexer options, using picocli.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@Command(name = "WARCIndexer", description = "Options for the WARCIndexer", mixinStandardHelpOptions = true, separator = " ")
public class WARCHadoopIndexerOptions extends WARCIndexerOptions {

    @Option(names = "-i", description = "Local input file that contains a list of HDFS paths to WARCs to be indexed.", required = true)
    public File input;

    @Option(names = "-o", description = "HDFS folder the job should write to.", required = true)
    public String output;

    @Option(names = { "-R",
    "--num-reducers" }, description = "Number of reducer tasks to use. Default is ${DEFAULT-VALUE}.", defaultValue = "5")
public int num_reducers;

    @Option(names = { "-w",
            "--wait" }, description = "Wait for job to finish.", defaultValue = "false")
    public boolean wait;

}
