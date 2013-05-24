package uk.bl.wap.util.solr;

/**
 * Writable wrapper for SolrInputDocument.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Writable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

public class WritableSolrRecord implements Writable, Serializable {
	private static final long serialVersionUID = -3409886058494054406L;

	public SolrInputDocument doc = new SolrInputDocument();

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

	public String toXml() {
		return ClientUtils.toXML( doc );
	}

	/**
	 * Add any non-null string properties, stripping control characters if present.
	 * 
	 * @param solr_property
	 * @param value
	 */
	public void addField( String solr_property, String value ) {
		if( value != null )
			doc.addField( solr_property, value.trim().replaceAll( "\\p{Cntrl}", "" ) );
	}
}