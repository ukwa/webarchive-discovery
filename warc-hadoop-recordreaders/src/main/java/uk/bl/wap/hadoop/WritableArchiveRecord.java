package uk.bl.wap.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;

public class WritableArchiveRecord implements Writable {
	private ArchiveRecord record = null;
	private HashMap<String, String> headers = new HashMap<String, String>();
	
	public static final String BL_STATUS_CODE_HEADER = "bl_status";

	public WritableArchiveRecord() {}

	public WritableArchiveRecord( ArchiveRecord record ) throws IOException {
		this.record = record;
	}

	public void setRecord( ArchiveRecord record ) throws IOException {
		this.record = record;
	}

	public ArchiveRecord getRecord() {
		return record;
	}

	public byte[] getPayload() throws IOException {
		BufferedInputStream input = new BufferedInputStream( record );
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		int ch;
		byte[] buffer = new byte[ 1048576 ];
		try {
			while( ( ch = input.read( buffer ) ) >= 0 ) {
				output.write( buffer, 0, ch );
			}
		} catch( IndexOutOfBoundsException i ) {
			// Invalid Content-Length throws this.
		}
		return output.toByteArray();
	}
	
	public InputStream getPayloadAsStream() {
		return record;
	}

	public void setHttpHeaders( String httpHeaders ) {
		BufferedReader input = new BufferedReader( new StringReader( httpHeaders ) );
		String line;
		String[] values;
		try {
			headers.put( BL_STATUS_CODE_HEADER, input.readLine() );
			while( ( line = input.readLine() ) != null ) {
				values = line.split( ": ", 2 );
				if( values.length == 2 )
					headers.put( values[ 0 ].toLowerCase(), values[ 1 ] );
			}
		} catch( IOException i ) {
			i.printStackTrace();
		}
	}

	public String getHttpHeader( String header ) {
		return headers.get( header.toLowerCase() );
	}

	@Override
	public void readFields( DataInput input ) throws IOException {
		record = ( WARCRecord ) input;
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		if( record != null ) {
			int ch;
			byte[] buffer = new byte[ 1048576 ];
			while( ( ch = record.read( buffer ) ) >= 0 ) {
				output.write( buffer, 0, ch );
			}
		}
	}

}
