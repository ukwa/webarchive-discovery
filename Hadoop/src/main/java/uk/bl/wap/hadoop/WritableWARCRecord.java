package uk.bl.wap.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.archive.io.warc.WARCRecord;

public class WritableWARCRecord implements Writable {
	private WARCRecord warcrecord = null;
	private byte[] payload = new byte[ 0 ];
	private HashMap<String, String> headers = new HashMap<String, String>();

	public WritableWARCRecord() {}

	public WritableWARCRecord( WARCRecord record ) throws IOException {
		this.warcrecord = record;
		this.setPayload( record );
	}

	public void setRecord( WARCRecord record ) throws IOException {
		this.warcrecord = record;
		this.setPayload( record );
	}

	public WARCRecord getRecord() {
		return warcrecord;
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setHttpHeaders( String httpHeaders ) {
		BufferedReader input = new BufferedReader( new StringReader( httpHeaders ) );
		String line;
		String[] values;
		try {
			headers.put( "bl_status", input.readLine() );
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
		warcrecord = ( WARCRecord ) input;
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		if( warcrecord != null ) {
			int ch;
			byte[] buffer = new byte[ 1048576 ];
			while( ( ch = warcrecord.read( buffer ) ) >= 0 ) {
				output.write( buffer, 0, ch );
			}
		}
	}

	private void setPayload( WARCRecord record ) throws IOException {
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
		this.payload = output.toByteArray();
	}
}
