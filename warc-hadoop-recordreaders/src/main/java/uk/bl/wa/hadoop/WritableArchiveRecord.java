package uk.bl.wa.hadoop;

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

	public byte[] getPayload(int max_buffer_size) throws IOException {
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
