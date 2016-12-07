package uk.bl.wa.hadoop.hosts;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@SuppressWarnings( "deprecation" )
public class HostsReportMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	Pattern pattern = Pattern.compile( "^https?://([^/]+)/.*$" );
	Matcher matcher;

	@Override
	public void map( LongWritable offset, Text log, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		// crawl.log
		// LogTimestamp StatusCode Size URI DiscoveryPath Referrer MIME ThreadID RequestTimestamp+Duration Digest - Annotations
		String[] entries = log.toString().split( "\\s+" );
		if( entries.length != 12 )
			return;
		// Skip invalid responses
		if( entries[ 1 ].startsWith( "-" ) && !entries[ 1 ].equals( "-9998" ) ) {
			return;
		}
		// hosts-report.txt
		// #urls #bytes ^host #robots #^remaining #^novel-urls #^novel-bytes #dup-by-hash-urls #dup-by-hash-bytes #^not-modified-urls #^not-modified-bytes
		StringBuilder sb = new StringBuilder();
		// #urls
		sb.append( "1 " );
		// #bytes
		if( entries[ 2 ].equals( "-" ) )
			sb.append( "0 " );
		else
			sb.append( entries[ 2 ] + " " );
		// #robots
		if( entries[ 1 ].equals( "-9998" ) )
			sb.append( "1 " );
		else
			sb.append( "0 " );
		// #dup-by-hash, #dup-by-hash-bytes
		if( entries[ 11 ].contains( "warcRevists:digest" ) ) {
			sb.append( "1 " );
			if( entries[ 2 ].equals( "-" ) )
				sb.append( "0 " );
			else
				sb.append( entries[ 2 ] + " " );
		} else {
			sb.append( "0 " );
			sb.append( "0 " );
		}
		String host;
		if( entries[ 3 ].startsWith( "dns:" ) ) {
			host = entries[ 3 ].replace( "dns:", "" );
		} else {
			matcher = pattern.matcher( entries[ 3 ] );
			if( matcher.matches() ) {
				host = matcher.group( 1 );
			} else {
				return;
			}
		}
		output.collect( new Text( host ), new Text( sb.toString() ) );
	}
}
