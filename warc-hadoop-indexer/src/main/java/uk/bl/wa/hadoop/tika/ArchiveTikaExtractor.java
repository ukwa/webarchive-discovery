package uk.bl.wa.hadoop.tika;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

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

import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.util.solr.WritableSolrRecord;

/**
 * ArchiveTikExtractor
 * Extracts text using Tika from a series of (W)ARC files.
 * 
 * @author rcoram
 */

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaExtractor extends Configured implements Tool {

	public static final String CONFIG_PROPERTIES = "warc_indexer_config";
	
	private int numReducers;

	public int run( String[] args ) throws IOException {
		JobConf conf = new JobConf( getConf(), ArchiveTikaExtractor.class );
		String line = null;

		BufferedReader br = new BufferedReader( new FileReader( args[ 0 ] ) );
		while( ( line = br.readLine() ) != null ) {
			FileInputFormat.addInputPath( conf, new Path( line ) );
		}
		br.close();

		FileOutputFormat.setOutputPath( conf, new Path( args[ 1 ] ) );

		this.setProperties( conf );
		conf.setJobName( args[ 0 ] + "_" + System.currentTimeMillis() );
		conf.setInputFormat( ArchiveFileInputFormat.class );
		conf.setMapperClass( ArchiveTikaMapper.class );
		conf.setReducerClass( ArchiveTikaReducer.class );
		conf.setOutputFormat( TextOutputFormat.class );
		conf.set( "map.output.key.field.separator", "" );

		conf.setOutputKeyClass( Text.class );
		conf.setOutputValueClass( Text.class );
		conf.setMapOutputValueClass( WritableSolrRecord.class );
		conf.setNumReduceTasks( numReducers );
		JobClient client = new JobClient( conf );
		client.submitJob( conf );
//		 JobClient.runJob( conf );
		return 0;
	}

	public static void main( String[] args ) throws Exception {
		if( !( args.length > 0 ) ) {
			System.out.println( "Need input file.list and output dir!" );
			System.exit( 0 );

		}
		int ret = ToolRunner.run( new ArchiveTikaExtractor(), args );

		System.exit( ret );
	}

	private void setProperties( JobConf conf ) throws IOException {
		// Store application properties where the mappers/reducers can access them
		Config index_conf = ConfigFactory.load();
		conf.set( CONFIG_PROPERTIES, index_conf.toString());

		// Also set mapred speculative execution off:
		conf.set( "mapred.reduce.tasks.speculative.execution", "false" );

		// Reducer count dependent on concurrent HTTP connections to Solr server.
		try {
			numReducers = index_conf.getInt( "warc.hadoop.num_reducers" );
		} catch( NumberFormatException n ) {
			numReducers = 10;
		}
	}
}
