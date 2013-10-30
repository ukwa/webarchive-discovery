package uk.bl.wa.hadoop.indexer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

import uk.bl.wa.util.solr.SolrRecord;

/**
 * Writable wrapper for SolrRecord.
 */
public class WritableSolrRecord  implements Writable, Serializable {
	private static final long serialVersionUID = -3409886058494054406L;
	
	private SolrRecord sr = new SolrRecord();

	WritableSolrRecord() { }

	public WritableSolrRecord( SolrRecord sr ) {
		this.sr = sr;
	}

	@Override
	public void readFields( DataInput input ) throws IOException {
		int length = input.readInt();
		byte[] bytes = new byte[ length ];
		input.readFully( bytes );
		this.sr.doc = ( SolrInputDocument ) SerializationUtils.deserialize( bytes );
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		byte[] bytes = SerializationUtils.serialize( this.sr.doc );
		output.writeInt( bytes.length );
		output.write( bytes );
	}
	
	public SolrRecord getSolrRecord() {
		return this.sr;
	}

}