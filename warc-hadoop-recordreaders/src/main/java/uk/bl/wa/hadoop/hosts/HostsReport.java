package uk.bl.wa.hadoop.hosts;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;

@SuppressWarnings( "deprecation" )
public class HostsReport extends Configured implements Tool {
	private static Logger log = Logger.getLogger( HostsReport.class.getName() );

	@Override
	public int run( String[] args ) throws Exception {
		JobConf conf = new JobConf( getConf(), HostsReport.class );

		log.info( "Adding logs..." );
		String line;
		BufferedReader br = new BufferedReader( new FileReader( args[ 0 ] ) );
		while( ( line = br.readLine() ) != null ) {
			log.info( "Adding " + line );
			FileInputFormat.addInputPath( conf, new Path( line ) );
		}
		br.close();

		FileOutputFormat.setOutputPath( conf, new Path( args[ 1 ] ) );
		conf.setJarByClass( HostsReport.class );
		conf.setInputFormat( TextInputFormat.class );
		conf.setMapperClass( HostsReportMapper.class );
		conf.setMapOutputKeyClass( Text.class );
		conf.setMapOutputValueClass( Text.class );
		conf.setCombinerClass( HostsReportReducer.class );
		conf.setReducerClass( HostsReportReducer.class );
		conf.setOutputFormat( TextOutputFormat.class );

		JobClient.runJob( conf );
		return 0;
	}

	public static void main( String[] args ) throws Exception {
		if( args.length != 2 ) {
			System.out.println( "Need <input file list> and <output dir>!" );
			System.exit( 1 );

		}
		int ret = ToolRunner.run( new HostsReport(), args );
		System.exit( ret );
	}
}
