package uk.bl.wap.util.solr;

import java.io.StringReader;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.common.SolrInputDocument;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.restlet.data.Response;

public class WctEnricher {
	private static final String WctRestletUrl = "http://mosaic-private:9090/wctmeta/instanceInfo/";
	private WritableSolrRecord solr;
	private XMLInputFactory inputFactory = null;
	private XMLStreamReader xmlReader = null;

	public WctEnricher( String wctTiId ) {
		solr = new WritableSolrRecord();
		solr.doc.addField( WctFields.WCT_INSTANCE_ID, wctTiId );
		getWctMetadata( solr );
	}

	public WctEnricher() {
		solr = new WritableSolrRecord();
	}

	public SolrInputDocument getSolr() {
		return this.solr.doc;
	}

	private void getWctMetadata( WritableSolrRecord solr ) {
		Client client = new Client( Protocol.HTTP );
		Response response = client.get( WctRestletUrl + this.solr.doc.getFieldValue( WctFields.WCT_INSTANCE_ID ) );
		this.read( response.getEntityAsText() );
	}

	public void addWctMetadata( WritableSolrRecord in ) {
		in.doc.addField( WctFields.WCT_TARGET_ID, this.solr.doc.getFieldValue( WctFields.WCT_TARGET_ID ) );
		in.doc.addField( WctFields.WCT_TITLE, this.solr.doc.getFieldValue( WctFields.WCT_TITLE ) );
		in.doc.addField( WctFields.WCT_HARVEST_DATE, this.solr.doc.getFieldValue( WctFields.WCT_HARVEST_DATE ) );
		in.doc.addField( WctFields.WCT_COLLECTIONS, this.solr.doc.getFieldValue( WctFields.WCT_COLLECTIONS ) );
		in.doc.addField( WctFields.WCT_AGENCY, this.solr.doc.getFieldValue( WctFields.WCT_AGENCY ) );
		in.doc.addField( WctFields.WCT_SUBJECTS, this.solr.doc.getFieldValue( WctFields.WCT_SUBJECTS ) );
	}

	public void read( String s ) {
		inputFactory = XMLInputFactory.newInstance();
		StringReader sr = new StringReader( s );
		String tag = "";

		try {
			xmlReader = inputFactory.createXMLStreamReader( sr );
			while( xmlReader.hasNext() ) {
				Integer eventType = xmlReader.next();
				if( eventType.equals( XMLEvent.START_ELEMENT ) ) {
					tag = xmlReader.getLocalName();
				} else if( eventType.equals( XMLEvent.CHARACTERS ) ) {
					setTag( tag, xmlReader.getText() );
				}
			}
			xmlReader.close();
		} catch( Exception ex ) {
			ex.printStackTrace();
		}
	}

	public void setTag( String tag, String value ) {
		if( tag.equals( WctFields.WCT_INSTANCE_ID ) ) {
			this.solr.doc.addField( WctFields.WCT_INSTANCE_ID, value );
		} else if( tag.equals( WctFields.WCT_TARGET_ID ) ) {
			this.solr.doc.addField( WctFields.WCT_TARGET_ID, value );
		} else if( tag.equals( WctFields.WCT_HARVEST_DATE ) ) {
			this.solr.doc.addField( WctFields.WCT_HARVEST_DATE, value );
		} else if( tag.equals( WctFields.WCT_AGENCY ) ) {
			this.solr.doc.addField( WctFields.WCT_AGENCY, value );
		} else if( tag.equals( WctFields.WCT_COLLECTIONS ) ) {
			this.solr.doc.addField( WctFields.WCT_COLLECTIONS, value );
		} else if( tag.equals( WctFields.WCT_SUBJECTS ) ) {
			this.solr.doc.addField( WctFields.WCT_SUBJECTS, value );
		}
	}
}
