/**
 * 
 */
package uk.bl.wa.util.solr;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrRecord {


	public SolrInputDocument doc = new SolrInputDocument();

	public String toXml() {
		return ClientUtils.toXML( doc );
	}
	
	private static int MAX_FIELD_LEN = 200;
	
	/**
	 * Remove control characters, nulls etc,
	 * 
	 * @param value
	 * @return
	 */
	private String removeControlCharacters( String value ) {
		return value.trim().replaceAll( "\\p{Cntrl}", "" );
	}
	
	/**
	 * Also shorten to avoid bad data filling 'small' fields with 'big' data.
	 * 
	 * @param value
	 * @return
	 */
	private String sanitizeString( String solr_property, String value ) {
		if( ! solr_property.equals( SolrFields.SOLR_EXTRACTED_TEXT ) ) {
			if( value.length() > MAX_FIELD_LEN ) {
				value = value.substring(0, MAX_FIELD_LEN);
			}
		}
		return removeControlCharacters(value);
	}

	/**
	 * Add any non-null string properties, stripping control characters if present.
	 * 
	 * @param solr_property
	 * @param value
	 */
	public void addField( String solr_property, String value ) {
		if( value != null )
			doc.addField( solr_property, sanitizeString(solr_property, value) );
	}

	/**
	 * Set instead of adding fields.
	 * 
	 * @param solr_property
	 * @param value
	 */
	public void setField( String solr_property, String value ) {
		if( value != null )
			doc.setField( solr_property, sanitizeString(solr_property, value) );
	}
	
}
