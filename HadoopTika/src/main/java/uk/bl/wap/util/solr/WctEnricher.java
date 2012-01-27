package uk.bl.wap.util.solr;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;

import uk.bl.wap.util.solr.SolrRecord;

public class WctEnricher {
	private SolrRecord solr;
	static String WctRestletUrl = "http://mosaic-private:9090/wctmeta/instanceInfo/";
	private XMLInputFactory inputFactory = null;
	private XMLStreamReader xmlReader = null;

	public WctEnricher( String wctTiId ) {
		solr = new SolrRecord();
		solr.setWctInstanceId( wctTiId );
		enrich( solr );
	}

	public WctEnricher() {
		solr = new SolrRecord();
	}

	public static void main( String[] args ) throws IOException {
		WctEnricher wct = new WctEnricher( "100004" );
		SolrRecord sr = new SolrRecord();
		sr.setTitle( "One" );
		SolrRecord sr2 = new SolrRecord();
		sr2.setTitle( "two" );
		wct.populate( sr );
		System.out.println( sr.toXml() );
		wct.populate( sr2 );
		System.out.println( sr2.toXml() );
	}

	public void populate( SolrRecord sin ) {
		sin.setWctTargetId( solr.getWctTargetId() );
		sin.setWctTitle( solr.getWctTitle() );
		sin.setWctHarvestDate( solr.getWctHarvestDate() );
		sin.setWctCollections( solr.getWctCollections() );
		sin.setWctAgency( solr.getWctAgency() );
		sin.setWctSubjects( solr.getWctSubjects() );
	}

	private void enrich( SolrRecord sr ) {
		ClientResource resource = new ClientResource( WctRestletUrl + sr.getWctInstanceId() );  
		Representation resp = resource.get();
		try {
			this.read( resp.getText() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	public SolrRecord getSolr() {
		return this.solr;
	}

	public void setTag( String tag, String value ) {
		if( tag.equals( "wct_target_id" ) ) {
			solr.setWctTargetId( value );
		} else if( tag.equals( "wct_title" ) ) {
			solr.setWctTitle( value );
		} else if( tag.equals( "wct_harvest_date" ) ) {
			solr.setWctHarvestDate( value );
		} else if( tag.equals( "wct_agency" ) ) {
			solr.setWctAgency( value );
		} else if( tag.equals( "wct_collections" ) ) {
			solr.setWctCollections( value );
		} else if( tag.equals( "wct_subjects" ) ) {
			solr.setWctSubjects( value );
		}
	}
}
