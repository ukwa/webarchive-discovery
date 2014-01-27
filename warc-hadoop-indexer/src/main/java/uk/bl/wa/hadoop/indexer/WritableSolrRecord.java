package uk.bl.wa.hadoop.indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

import uk.bl.wa.util.solr.SolrRecord;

/**
 * Writable wrapper for SolrRecord.
 */
public class WritableSolrRecord  implements Writable {
	private static final long serialVersionUID = -3409886058494054406L;
	public static final String ARC_TYPE = "arc";
	
	private SolrRecord sr = new SolrRecord();
	private String recordType;

	WritableSolrRecord() { }

	public WritableSolrRecord( SolrRecord sr ) {
		this.sr = sr;
	}

	public void setType( String type ) {
		if( type == null )
			recordType = ARC_TYPE;
		else
			recordType = type;
	}

	public String getType() {
		return recordType;
	}

	@Override
	public void readFields( DataInput input ) throws IOException {
		int length = input.readInt();
		byte[] bytes = new byte[ length ];
		input.readFully( bytes );
		this.setType( new String( bytes ) );

		length = input.readInt();
		bytes = new byte[ length ];
		input.readFully( bytes );
		this.sr.doc = ( SolrInputDocument ) SerializationUtils.deserialize( bytes );
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		byte[] bytes = SerializationUtils.serialize( this.sr.doc );
		output.writeInt( recordType.length() );
		output.write( recordType.getBytes() );
		output.writeInt( bytes.length );
		output.write( bytes );
	}
	
	public SolrRecord getSolrRecord() {
		return this.sr;
	}
}