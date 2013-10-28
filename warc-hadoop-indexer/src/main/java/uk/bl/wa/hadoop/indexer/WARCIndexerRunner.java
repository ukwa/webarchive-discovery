package uk.bl.wa.hadoop.indexer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.util.solr.WritableSolrRecord;

/**
 * WARCIndexerRunner
 * Extracts text/metadata using  from a series of Archive files.
 * 
 * @author rcoram
 */

@SuppressWarnings( { "deprecation" } )
public class WARCIndexerRunner extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog( WARCIndexerRunner.class );
	private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-d] [Dump config.]";
	private static final String CLI_HEADER = "WARCIndexerRunner - MapReduce method for extracing metadata/text from Archive Records";
	public static final String CONFIG_PROPERTIES = "warc_indexer_config";

	private String inputPath;
	private String outputPath;
	private boolean dumpConfig;

	/**
	 * 
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws ParseException 
	 */
	protected void createJobConf( JobConf conf, String[] args ) throws IOException, ParseException {
		// Parse the command-line parameters.
		this.setup( args );

		// Store application properties where the mappers/reducers can access them
		Config index_conf = ConfigFactory.load();
		conf.set( CONFIG_PROPERTIES, index_conf.withOnlyPath( "warc" ).root().render( ConfigRenderOptions.concise() ) );
		LOG.info( "Loaded warc config." );

		if( this.dumpConfig ) {
			LOG.info( "Dumping warc config." );
			System.out.println( index_conf.withOnlyPath( "warc" ).root().render() );
			System.exit( 0 );
		}

		// Also set mapred speculative execution off:
		conf.set( "mapred.reduce.tasks.speculative.execution", "false" );

		// Reducer count dependent on concurrent HTTP connections to Solr server.
		int numReducers = 1;
		try {
			numReducers = index_conf.getInt( "warc.hadoop.num_reducers" );
		} catch( NumberFormatException n ) {
			numReducers = 10;
		}

		// Add input paths:
		LOG.info( "Reading input files..." );
		String line = null;
		BufferedReader br = new BufferedReader( new FileReader( this.inputPath ) );
		while( ( line = br.readLine() ) != null ) {
			FileInputFormat.addInputPath( conf, new Path( line ) );
		}
		br.close();
		LOG.info( "Read input files." );

		FileOutputFormat.setOutputPath( conf, new Path( this.outputPath ) );

		conf.setJobName( args[ 0 ] + "_" + System.currentTimeMillis() );
		conf.setInputFormat( ArchiveFileInputFormat.class );
		conf.setMapperClass( WARCIndexerMapper.class );
		conf.setReducerClass( WARCIndexerReducer.class );
		conf.setOutputFormat( TextOutputFormat.class );
		conf.set( "map.output.key.field.separator", "" );

		conf.setOutputKeyClass( Text.class );
		conf.setOutputValueClass( Text.class );
		conf.setMapOutputValueClass( WritableSolrRecord.class );
		conf.setNumReduceTasks( numReducers );
	}

	/**
	 * 
	 * Run the job:
	 * 
	 */
	public int run( String[] args ) throws IOException, ParseException {
		// Set up the base conf:
		JobConf conf = new JobConf( getConf(), WARCIndexerRunner.class );

		// Get the job configuration:
		this.createJobConf( conf, args );

		// Submit it:
		JobClient client = new JobClient( conf );
		client.submitJob( conf );

		return 0;
	}

	@SuppressWarnings( "static-access" )
	private void setup( String[] args ) throws ParseException {
		Options options = new Options();
		options.addOption( "i", true, "input file list" );
		options.addOption( "o", true, "output directory" );
		options.addOption( "d", false, "dump configuration" );
		options.addOption( OptionBuilder.withArgName( "property=value" ).hasArgs( 2 ).withValueSeparator().withDescription( "use value for given property" ).create( "D" ) );

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse( options, args );
		if( !cmd.hasOption( "i" ) || !cmd.hasOption( "o" ) ) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.setWidth( 80 );
			helpFormatter.printHelp( CLI_USAGE, CLI_HEADER, options, "" );
			System.exit( 1 );
		}
		this.inputPath = cmd.getOptionValue( "i" );
		this.outputPath = cmd.getOptionValue( "o" );
		this.dumpConfig = cmd.hasOption( "d" );
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception {
		int ret = ToolRunner.run( new WARCIndexerRunner(), args );
		System.exit( ret );
	}

}
