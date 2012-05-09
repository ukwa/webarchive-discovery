package uk.bl.wap.hadoop.regex;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_URI;
import static org.archive.io.warc.WARCConstants.HEADER_KEY_DATE;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wap.hadoop.WritableArchiveRecord;

@SuppressWarnings( { "deprecation" } )
public class WARCRegexMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, Text> {
	private Pattern pattern;
	
	
	public WARCRegexMapper() {}

	@Override
	public void configure( JobConf job ) {
        this.pattern = Pattern.compile( job.get( WARCRegexIndexer.REGEX_PATTERN_PARAM ) ); 
    }
	
	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();

		Matcher matcher = pattern.matcher( new String( value.getPayload(), "UTF-8" ) );
		while( matcher.find() ) {
			String newKey = ( String ) header.getHeaderValue( HEADER_KEY_DATE ) + "/" + ( String ) header.getHeaderValue( HEADER_KEY_URI );
			output.collect( new Text( newKey ), new Text( matcher.group( 0 ) ) );
		}
	}
	
}
