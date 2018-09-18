package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2018 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

public class WctEnricher {
    private static final String WctRestletUrl = "http://mosaic-private:9090/wctmeta/instanceInfo/";
    private SolrRecord solr;
    private XMLInputFactory inputFactory = null;
    private XMLStreamReader xmlReader = null;

    public WctEnricher( String archiveName ) {
        String wctID = this.getWctTi( archiveName );
        solr = SolrRecordFactory.createFactory(null).createRecord(); // Never reduces field length size
        solr.setField( WctFields.WCT_INSTANCE_ID, wctID );
        getWctMetadata( solr );
    }

    private void getWctMetadata( SolrRecord solr ) {
        
        ClientResource cr = new ClientResource( WctRestletUrl + this.solr.getFieldValue( WctFields.WCT_INSTANCE_ID ) );
        try {
            this.read( cr.get().getStream() );
        } catch (ResourceException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addWctMetadata( SolrRecord in ) {
        in.addField( WctFields.WCT_TARGET_ID, this.solr.getFieldValue( WctFields.WCT_TARGET_ID ).toString() );
        in.addField( WctFields.WCT_TITLE, this.solr.getFieldValue( WctFields.WCT_TITLE ).toString() );
        in.addField( WctFields.WCT_HARVEST_DATE, this.solr.getFieldValue( WctFields.WCT_HARVEST_DATE ).toString() );
        in.addField( WctFields.WCT_COLLECTIONS, this.solr.getFieldValue( WctFields.WCT_COLLECTIONS ).toString() );
        in.addField( WctFields.WCT_AGENCY, this.solr.getFieldValue( WctFields.WCT_AGENCY ).toString() );
        in.addField( WctFields.WCT_SUBJECTS, this.solr.getFieldValue( WctFields.WCT_SUBJECTS ).toString() );
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
            this.solr.addField( WctFields.WCT_INSTANCE_ID, value );
        } else if( tag.equals( WctFields.WCT_TARGET_ID ) ) {
            this.solr.addField( WctFields.WCT_TARGET_ID, value );
        } else if( tag.equals( WctFields.WCT_HARVEST_DATE ) ) {
            this.solr.addField( WctFields.WCT_HARVEST_DATE, value );
        } else if( tag.equals( WctFields.WCT_AGENCY ) ) {
            this.solr.addField( WctFields.WCT_AGENCY, value );
        } else if( tag.equals( WctFields.WCT_COLLECTIONS ) ) {
            this.solr.addField( WctFields.WCT_COLLECTIONS, value );
        } else if( tag.equals( WctFields.WCT_SUBJECTS ) ) {
            this.solr.addField( WctFields.WCT_SUBJECTS, value );
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
