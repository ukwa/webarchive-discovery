package uk.bl.wa.util.solr;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.common.SolrInputDocument;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

public class WctEnricher {
	private static final String WctRestletUrl = "http://mosaic-private:9090/wctmeta/instanceInfo/";
	private SolrRecord solr;
	private XMLInputFactory inputFactory = null;
	private XMLStreamReader xmlReader = null;

	public WctEnricher( String archiveName ) {
		String wctID = this.getWctTi( archiveName );
		solr = new SolrRecord();
		solr.doc.setField( WctFields.WCT_INSTANCE_ID, wctID );
		getWctMetadata( solr );
	}

	public SolrInputDocument getSolr() {
		return this.solr.doc;
	}

	private void getWctMetadata( SolrRecord solr ) {
		
		ClientResource cr = new ClientResource( WctRestletUrl + this.solr.doc.getFieldValue( WctFields.WCT_INSTANCE_ID ) );
		try {
			this.read( cr.get().getStream() );
		} catch (ResourceException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addWctMetadata( SolrRecord in ) {
		in.doc.addField( WctFields.WCT_TARGET_ID, this.solr.doc.getFieldValue( WctFields.WCT_TARGET_ID ) );
		in.doc.addField( WctFields.WCT_TITLE, this.solr.doc.getFieldValue( WctFields.WCT_TITLE ) );
		in.doc.addField( WctFields.WCT_HARVEST_DATE, this.solr.doc.getFieldValue( WctFields.WCT_HARVEST_DATE ) );
		in.doc.addField( WctFields.WCT_COLLECTIONS, this.solr.doc.getFieldValue( WctFields.WCT_COLLECTIONS ) );
		in.doc.addField( WctFields.WCT_AGENCY, this.solr.doc.getFieldValue( WctFields.WCT_AGENCY ) );
		in.doc.addField( WctFields.WCT_SUBJECTS, this.solr.doc.getFieldValue( WctFields.WCT_SUBJECTS ) );
	}

	public void read( InputStream s ) {
		inputFactory = XMLInputFactory.newInstance();
		String tag = "";

		try {
			xmlReader = inputFactory.createXMLStreamReader( s );
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
	
	private String getWctTi( String warcName ) {
		Pattern pattern = Pattern.compile( "^[A-Z]+-\\b([0-9]+)\\b.*\\.w?arc(\\.gz)?$" );
		Matcher matcher = pattern.matcher( warcName );
		if( matcher.matches() ) {
			return matcher.group( 1 );
		}
		return "";
	}	


}
