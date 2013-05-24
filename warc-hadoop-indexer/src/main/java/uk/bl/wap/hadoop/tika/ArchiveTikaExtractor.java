package uk.bl.wap.hadoop.tika;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wap.hadoop.ArchiveFileInputFormat;
import uk.bl.wap.hadoop.TextOutputFormat;
import uk.bl.wap.util.solr.WritableSolrRecord;

/**
 * ArchiveTikExtractor
 * Extracts text using Tika from a series of (W)ARC files.
 * 
 * @author rcoram
 */

@SuppressWarnings( { "deprecation" } )
public class ArchiveTikaExtractor extends Configured implements Tool {
	private static final String CONFIG = "/hadoop_utils.config";
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
//		JobClient.runJob( conf );
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
		Properties properties = new Properties();
		properties.load( this.getClass().getResourceAsStream( ( CONFIG ) ) );
		conf.set( "mapred.reduce.tasks.speculative.execution", "false" );

		conf.set( "solr.server", properties.getProperty( "solr_server" ) );
		conf.set( "solr.batch.size", properties.getProperty( "solr_batch_size" ) );
		conf.set( "solr.threads", properties.getProperty( "solr_threads" ) );

		conf.set( "record.exclude.url", properties.getProperty( "url_exclude" ) );
		conf.set( "record.size.max", properties.getProperty( "max_payload_size" ) );
		conf.set( "record.include.response", properties.getProperty( "response_include" ) );
		conf.set( "record.include.protocol", properties.getProperty( "protocol_include" ) );

		conf.set( "tika.exclude.mime", properties.getProperty( "mime_exclude" ) );
		conf.set( "tika.timeout", properties.getProperty( "parse_timeout" ) );

		// Reducer count dependent on concurrent HTTP connections to Solr server.
		try {
			numReducers = Integer.parseInt( properties.getProperty( "hadoop_reducers" ) );
		} catch( NumberFormatException n ) {
			numReducers = 10;
		}
	}
}
