package uk.bl.wap.hadoop.regex;

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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wap.hadoop.ArchiveFileInputFormat;

/**
 * WARCRegexIndexer
 * Match records in a series of WARC files to a given regular expression. 
 * 
 * @author rcoram
 */

@SuppressWarnings( { "deprecation" } )
public class WARCRegexIndexer extends Configured implements Tool {
	
	public static final String REGEX_PATTERN_PARAM = "regex.pattern";

	public int run( String[] args ) throws IOException {
		JobConf conf = new JobConf( getConf(), WARCRegexIndexer.class );
		String line = null;

		BufferedReader br = new BufferedReader( new FileReader( args[ 0 ] ) );

		while( ( line = br.readLine() ) != null ) {
			FileInputFormat.addInputPath( conf, new Path( line ) );
		}

		FileOutputFormat.setOutputPath( conf, new Path( args[ 1 ] ) );
		
		conf.set( REGEX_PATTERN_PARAM, args[ 2 ] );

		conf.setJobName( args[ 0 ] + "_" + System.currentTimeMillis() );
		conf.setInputFormat( ArchiveFileInputFormat.class );
		conf.setMapperClass( WARCRegexMapper.class );
		conf.setReducerClass( WARCRegexReducer.class );
		conf.setNumReduceTasks( 1 );
		conf.setOutputFormat( TextOutputFormat.class );

		conf.setOutputKeyClass( Text.class );
		conf.setOutputValueClass( Text.class );
		JobClient.runJob( conf );
		return 0;

	}

	public static void main( String[] args ) throws Exception {
		if( args.length != 3 ) {
			System.out.println( "Need <input file list>, <output dir> and <regular expression>!" );
			System.exit( 1 );

		}
		int ret = ToolRunner.run( new WARCRegexIndexer(), args );
		System.exit( ret );
	}
}
