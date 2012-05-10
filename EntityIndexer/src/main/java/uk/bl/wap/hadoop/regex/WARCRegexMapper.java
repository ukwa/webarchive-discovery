package uk.bl.wap.hadoop.regex;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.URIException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.tika.Tika;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wap.hadoop.WritableArchiveRecord;

@SuppressWarnings( { "deprecation" } )
public class WARCRegexMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, Text> {
	
	private static Logger log = Logger.getLogger(WARCRegexMapper.class.getName());
	
	private Pattern pattern;
	
	Tika tika = new Tika();
	
	public WARCRegexMapper() {}

	@Override
	public void configure( JobConf job ) {
        this.pattern = Pattern.compile( job.get( WARCRegexIndexer.REGEX_PATTERN_PARAM ) ); 
    }
	
	@Override
	public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
		ArchiveRecordHeader header = value.getRecord().getHeader();

		/*
		// Determine the type:
		try {
			String tikaType = tika.detect(value.getPayload());
			// return without collecting anything if this is neither HTML or XHTML:
			if( ! tikaType.startsWith("application/xhtml+xml") &&
					! tikaType.startsWith("text/html") );
			return;
		} catch( Throwable e ) {
			log.error( "Tika.detect failed:" + e.getMessage() );
			//e.printStackTrace();
		}
		*/
		
		// Generate the key:
		String newKey = "0/unknown";			
		if( !header.getHeaderFields().isEmpty() ) {
			newKey = header.getDate() + "/" + header.getUrl();
			// Reduce this to just the year and the host:
			//String year = extractYear(header.getDate());
			//String host = extractHost(header.getUrl());
			//newKey = year + "/" + host;
		}
		
		// Collect the matches:
		Matcher matcher = pattern.matcher( new String( value.getPayload(), "UTF-8" ) );
		while( matcher.find() ) {
			output.collect( new Text( newKey ), new Text( matcher.group( 0 ) ) );
		}
	}
	
	/**
	 * 
	 * @param timestamp
	 * @return
	 */
	private static String extractYear(String timestamp) {
		String waybackYear = "unknown-year";
		String waybackDate = timestamp.replaceAll( "[^0-9]", "" );
		if( waybackDate != null ) 
			waybackYear = waybackDate.substring(0,4);
		return waybackYear;
	}
	
	/**
	 * 
	 * @param url
	 * @return
	 */
	private static String extractHost(String url) {
		String host = "unknown.host";
		URI uri = null;
		// Attempt to parse:
		try {
			uri = new URI(url,false);
			// Extract domain:
			host = uri.getHost();
		} catch ( Exception e ) {
			// Return a special hostname if parsing failed:
			host = "malformed.host";
		}
		return host;
	}
}
