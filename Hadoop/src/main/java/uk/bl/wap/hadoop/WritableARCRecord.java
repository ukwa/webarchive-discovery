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
import org.archive.io.arc.ARCRecord;

public class WritableARCRecord implements Writable {
	private ARCRecord arcrecord = null;
	private byte[] payload = new byte[ 0 ];
	private HashMap<String, String> headers = new HashMap<String, String>();

	public WritableARCRecord() {}

	public WritableARCRecord( ARCRecord arcrecord ) throws IOException {
		this.arcrecord = arcrecord;
		this.setPayload( arcrecord );
	}

	public void setRecord( ARCRecord arcrecord ) throws IOException {
		this.arcrecord = arcrecord;
		this.setPayload( arcrecord );
	}

	public ARCRecord getRecord() {
		return this.arcrecord;
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
		arcrecord = ( ARCRecord ) input;
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		if( arcrecord != null ) {
			int ch;
			byte[] buffer = new byte[ 1048576 ];
			while( ( ch = arcrecord.read( buffer ) ) >= 0 ) {
				output.write( buffer, 0, ch );
			}
		}
	}

	private void setPayload( ARCRecord record ) throws IOException {
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
