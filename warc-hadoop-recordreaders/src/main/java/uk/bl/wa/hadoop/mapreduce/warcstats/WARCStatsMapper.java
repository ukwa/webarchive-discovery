/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.warcstats;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;

import uk.bl.wa.hadoop.WritableArchiveRecord;

/**
 * @author Andrew.Jackson@bl.uk
 *
 */
public class WARCStatsMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, Text> {

	private static Log log = LogFactory.getLog(WARCStatsMapper.class);	

	@Override
	public void map(Text key, WritableArchiveRecord value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		ArchiveRecord record = value.getRecord();
		ArchiveRecordHeader header = record.getHeader();

		// Logging for debug info:
		log.debug("Processing @"+header.getOffset()+
				"+"+record.available()+","+header.getLength()+
				": "+header.getUrl());		
		for( String h : header.getHeaderFields().keySet()) {
			log.debug("ArchiveHeader: "+h+" -> "+header.getHeaderValue(h));
		}
		
		// count all records:
		output.collect( new Text("record-total"), new Text("RECORD-TOTAL"));
		// check type:
		output.collect( new Text("record-type"), new Text("WARC-RECORD-TYPE\t"+header.getHeaderValue( HEADER_KEY_TYPE )));
	    if( record instanceof WARCRecord ) {
			output.collect( new Text("record-type"), new Text("RECORD-TYPE-WARC") );	    	
	    } else if( record instanceof ARCRecord ) {
			output.collect( new Text("record-type"), new Text("RECORD-TYPE-ARC") );	    	
	    } else {
			output.collect( new Text("record-type"), new Text("RECORD-TYPE-UNKNOWN") );
	    }
	    
	    // Other stats:
		output.collect( new Text("content-types"), new Text("CONTENT-TYPE\t"+header.getMimetype()) );
		String date = header.getDate();
		if( date != null && date.length() > 4 ) {
			output.collect( new Text("content-types"), new Text("YEAR\t"+date.substring(0,4)) );
		} else {
			output.collect( new Text("malformed-date"), new Text("MALFORMED-DATE") );			
		}

		// TODO: Consider calling Normalisation.sanitiseWARCHeaderValue on the header.getUrl to guard against '<>'-encapsulation
		// URL:
		String uri = header.getUrl();
		if( uri != null ){ 
			UsableURI uuri = UsableURIFactory.getInstance(uri);
			// Hosts:
			if( "https".contains(uuri.getScheme()) ) {
				output.collect( new Text("record-hosts"), new Text("HOSTS\t"+uuri.getAuthority()) );
			}
		} else {
			output.collect( new Text("record-hosts"), new Text("NULL-URI-TOTAL") );			
		}

	}

}
