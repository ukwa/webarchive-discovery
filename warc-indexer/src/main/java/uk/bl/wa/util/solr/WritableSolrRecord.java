package uk.bl.wa.util.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;

/**
 * Writable wrapper for SolrRecord.
 */
public class WritableSolrRecord extends SolrRecord implements Writable, Serializable {
	private static final long serialVersionUID = -3409886058494054406L;

	@Override
	public void readFields( DataInput input ) throws IOException {
		int length = input.readInt();
		byte[] bytes = new byte[ length ];
		input.readFully( bytes );
		this.doc = ( SolrInputDocument ) SerializationUtils.deserialize( bytes );
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		byte[] bytes = SerializationUtils.serialize( doc );
		output.writeInt( bytes.length );
		output.write( bytes );
	}

}