package uk.bl.wap.hadoop.tika;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import uk.bl.wap.hadoop.TextOutputFormat;
import uk.bl.wap.hadoop.ARCFileInputFormat;
import uk.bl.wap.util.solr.SolrRecord;

/**
 * ARCTikExtractor
 * Extracts text using Tika from a series of ARC files.
 * 
 * @author rcoram
 */

@SuppressWarnings( { "deprecation" } )
public class ARCTikaExtractor extends Configured implements Tool {

	public int run( String[] args ) throws IOException {
		JobConf conf = new JobConf( getConf(), ARCTikaExtractor.class );
		String line = null;

		BufferedReader br = new BufferedReader( new FileReader( args[ 0 ] ) );

		HashMap<String, Integer> tiMap = new HashMap<String, Integer>();

		while( ( line = br.readLine() ) != null ) {
			FileInputFormat.addInputPath( conf, new Path( line ) );
			String val[] = line.split( "/" );
			// Count the number of Target Instances in this batch and set number
			// of reducers accordingly
			tiMap.put( this.getWctTi( val[ val.length - 1 ] ), 1 );
		}

		FileOutputFormat.setOutputPath( conf, new Path( args[ 1 ] ) );

		conf.setJobName( args[ 0 ] + "_" + System.currentTimeMillis() );
		conf.setInputFormat( ARCFileInputFormat.class );
		conf.setMapperClass( ARCTikaMapper.class );
		conf.setReducerClass( ArchiveTikaReducer.class );
		conf.setOutputFormat( TextOutputFormat.class );
		conf.set( "map.output.key.field.separator", "" );

		conf.setOutputKeyClass( Text.class );
		conf.setOutputValueClass( Text.class );
		conf.setMapOutputValueClass( SolrRecord.class );
		conf.setNumReduceTasks( tiMap.size() );
		JobClient.runJob( conf );
		return 0;

	}

	public static void main( String[] args ) throws Exception {
		if( !( args.length > 0 ) ) {
			System.out.println( "Need input file.list and output dir!" );
			System.exit( 0 );

		}
		int ret = ToolRunner.run( new ARCTikaExtractor(), args );

		System.exit( ret );
	}

	private String getWctTi( String arcName ) {
		Pattern pattern = Pattern.compile( "^BL-\\b([0-9]+)\\b.+\\.arc(\\.gz)?$" );
		Matcher matcher = pattern.matcher( arcName );
		if( matcher.matches() ) {
			return matcher.group( 1 );
		}
		return "";
	}
}
