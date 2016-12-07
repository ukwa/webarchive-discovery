package uk.bl.wa.hadoop.hosts;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings( "deprecation" )
public class HostsReportReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce( Text host, Iterator<Text> logs, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		long urls = 0L;
		long bytes = 0L;
		long robots = 0L;
		long dupbyhash = 0L;
		long dupbyhashbytes = 0L;
		while( logs.hasNext() ) {
			String[] entries = logs.next().toString().split( "\\s+" );
			urls += Long.parseLong( entries [ 0 ] );
			bytes += Long.parseLong( entries[ 1 ] );
			robots += Long.parseLong( entries[ 2 ] );
			dupbyhash += Long.parseLong( entries[ 3 ] );
			dupbyhashbytes += Long.parseLong( entries[ 4 ] );
		}
		String line = urls + " " + bytes + " " + robots + " " + dupbyhash + " " + dupbyhashbytes;
		output.collect( new Text( host ), new Text( line ) );
	}
}
