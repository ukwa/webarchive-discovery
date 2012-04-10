package uk.bl.wap.util.solr;

/**
 * Writable wrapper for SolrInputDocument.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class WritableSolrRecord implements Writable {
	public SolrInputDocument doc = new SolrInputDocument();
	
	@Override
	public void readFields( DataInput input ) throws IOException {
		this.doc = new SolrInputDocument();
		int length = 0;
		String key = "";
		String value = "";

		int keys = input.readInt();
		try {
			for( int i = 0; i < keys; i++ ) {
				length = input.readInt();
				byte[] bytes = new byte[ length ];
				input.readFully( bytes );
				key = new String( bytes );
		
				length = input.readInt();
				bytes = new byte[ length ];
				input.readFully( bytes );
				value = new String( bytes );
				
				this.doc.setField( key, value );
			}
		} catch( Exception e ) {
			System.err.println( "WritableSolrRecord.readFields(): " + e.getMessage() );
		}
	}

	@Override
	public void write( DataOutput output ) throws IOException {
		int keys = this.doc.getFieldNames().size();
		output.writeInt( keys );

		Iterator<SolrInputField> iterator = this.doc.iterator();
		while( iterator.hasNext() ) {
			SolrInputField field = iterator.next();
			writeBytes( output, field.getName() );
			writeBytes( output, ( String ) field.getValue() );			
		}
	}

	private void writeBytes( DataOutput output, String data ) throws IOException {
		if( data == null ) {
			output.writeInt( 0 );
		} else {
			output.writeInt( data.getBytes().length );
			output.write( data.getBytes() );
		}
	}

	public String toXml() {
		StringBuilder sb = new StringBuilder();
		sb.append( "<doc>" );

		Iterator<SolrInputField> iterator = this.doc.iterator();
		while( iterator.hasNext() ) {
			SolrInputField field = iterator.next();
			sb.append( "<" + ( String ) field.getName() + "><![CDATA[" );
			sb.append( field.getValue() );
			sb.append( "]]></" + ( String ) field.getName() + ">" );
		}
		sb.append( "</doc>" );
		return sb.toString();
	}
}