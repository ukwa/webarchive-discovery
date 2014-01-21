package uk.bl.wa.hadoop;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;

public class WritableArchiveRecord implements Writable, WritableComparable<WritableArchiveRecord> {
	private static Log log = LogFactory.getLog( WritableArchiveRecord.class );

	private ArchiveRecord record = null;

	public WritableArchiveRecord() {}

	public WritableArchiveRecord( ArchiveRecord record ) throws IOException {
		this.record = record;
	}

	public void setRecord( ArchiveRecord record ) throws IOException {
		this.record = record;
	}

	public ArchiveRecord getRecord() {
		log.info( "Calling getRecord()..." );
		return record;
	}

	public byte[] getPayload( int max_buffer_size ) throws IOException {
		log.info( "Calling getPayload(int)..." );
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
		log.info( "Calling getPayloadAsStream()..." );
		return record;
	}

	@Override
	public void readFields( DataInput input ) throws IOException {
		log.warn( "Calling readField(DataInput)..." );
		record = ( WARCRecord ) input;
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		log.warn( "Calling write(DataOutput)..." );
		if( record != null ) {
			int ch;
			byte[] buffer = new byte[ 1048576 ];
			while( ( ch = record.read( buffer ) ) >= 0 ) {
				output.write( buffer, 0, ch );
			}
		}
	}

	@Override
	public int compareTo( WritableArchiveRecord record ) {
		String toUrl = record.getRecord().getHeader().getUrl();
		String toHash = record.getRecord().getHeader().getDigest();
		String thisUrl = this.getRecord().getHeader().getUrl();
		String thisHash = this.getRecord().getHeader().getDigest();
		return ( thisHash + "/" + thisUrl ).compareTo( toHash + "/" + toUrl );
	}
}
