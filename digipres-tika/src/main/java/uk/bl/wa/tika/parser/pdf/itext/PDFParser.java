/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package uk.bl.wa.tika.parser.pdf.itext;

/*
 * #%L
 * digipres-tika
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import org.apache.jempbox.xmp.XMPMetadata;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.image.xmp.JempboxExtractor;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import uk.bl.wa.tika.parser.pdf.XMPSchemaPDFA;

import com.itextpdf.text.pdf.PdfDictionary;
import com.itextpdf.text.pdf.PdfName;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;

/**
 * PDF parser.
 * <p>
 * This parser can process also encrypted PDF documents if the required password is given as a part of the input metadata associated with a document. If no password is given, then this parser will try decrypting the document using the empty
 * password that's often used with PDFs.
 * 
 * FIXME Note that 'creator' as in 'author' overwrites the 'pdf:creator' (as in sofware app) somehow. 
 * 
 * @author Roger Coram, Andrew Jackson <Andrew.Jackson@bl.uk>
 */
public class PDFParser extends AbstractParser {

    /** Serial version UID */
    private static final long serialVersionUID = -752276948656079347L;

    /**
     * Metadata key for giving the document password to the parser.
     * 
     * @since Apache Tika 0.5
     */
    public static final String PASSWORD = "org.apache.tika.parser.pdf.password";

    private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton( MediaType.application( "pdf" ) );

    public Set<MediaType> getSupportedTypes( ParseContext context ) {
        return SUPPORTED_TYPES;
    }

    public static void main( String[] args ) {
        try {
            FileInputStream input = new FileInputStream( new File( "src/test/resources/simple-PDFA-1a.pdf" ) );
            OutputStream output = System.out; //new FileOutputStream( new File( "Z:/part-00001.xml" ) );
            PdfReader reader = new PdfReader( input );
            StringBuilder builder = new StringBuilder();

            Metadata metadata = new Metadata();
            PDFParser.extractMetadata( reader, metadata );
            builder.append( "<?xml version=\"1.0\" encoding=\"UTF-8\"?><wctdocs><![CDATA[" );
            builder.append( PDFParser.extractText( reader ) );
            builder.append( "]]></wctdocs>\n" );
            input.close();
            
            output.write( builder.toString().getBytes( "UTF-8" ) );            
            
            for( String key : metadata.names() ) {
                output.write( (key+" : "+metadata.get(key)+"\n").getBytes( "UTF-8" ) );
            }
            output.close();
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }

    public PDFParser() {}

    public void parse( InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context ) throws IOException, SAXException, TikaException {
        PdfReader reader = new PdfReader( stream );
        PDFParser.extractMetadata( reader, metadata );

        XHTMLContentHandler xhtml = new XHTMLContentHandler( handler, metadata );
        xhtml.startDocument();
        xhtml.startElement( "p" );
        xhtml.characters( new String( PDFParser.extractText( reader ).getBytes( "UTF-8" ), "UTF-8" ) );
        xhtml.endElement( "p" );
        xhtml.endDocument();
    }

    private static String extractText( PdfReader reader ) {
        StringBuilder output = new StringBuilder();
        try {
            int numPages = reader.getNumberOfPages();
            int page = 1;
            while( page <= numPages ) {
                output.append( PdfTextExtractor.getTextFromPage( reader, page ) );
                page++;
            }
        } catch( Exception e ) {
            System.err.println( "PDFParser.extractText(): " + e.getMessage() );
        }
        return output.toString();
    }

    private static void extractMetadata( PdfReader reader, Metadata metadata ) {
        try {
            HashMap<String, String> map = reader.getInfo();
            // Clone the PDF info:
            for( String key : map.keySet() ) {
                metadata.set( key.toLowerCase(), map.get( key ) );
            }
            // Add other data of interest:
            metadata.set("pdf:version", "1."+reader.getPdfVersion());
            metadata.set("pdf:numPages", ""+reader.getNumberOfPages());
            metadata.set("pdf:cryptoMode", ""+getCryptoModeAsString(reader));
            metadata.set("pdf:openedWithFullPermissions", ""+reader.isOpenedWithFullPermissions());
            metadata.set("pdf:encrypted", ""+reader.isEncrypted());
            metadata.set("pdf:metadataEncrypted", ""+reader.isMetadataEncrypted());
            metadata.set("pdf:128key", ""+reader.is128Key());
            metadata.set("pdf:tampered", ""+reader.isTampered());
            // Also grap XMP metadata, if present:
            byte[] xmpmd = reader.getMetadata();
            if( xmpmd != null ) {
                // This is standard Tika code for parsing standard stuff from the XMP:
                JempboxExtractor extractor = new JempboxExtractor(metadata);
                extractor.parse( new ByteArrayInputStream( xmpmd ) );
                // This is custom XMP-handling code:
                XMPMetadata xmp = XMPMetadata.load( new ByteArrayInputStream( xmpmd ) );
                // There is a special class for grabbing data in the PDF schema - not sure it will add much here:
                // Could parse xmp:CreatorTool and pdf:Producer etc. etc. out of here.
                //XMPSchemaPDF pdfxmp = xmp.getPDFSchema();
                // Added a PDF/A schema class:
                xmp.addXMLNSMapping(XMPSchemaPDFA.NAMESPACE, XMPSchemaPDFA.class);
                XMPSchemaPDFA pdfaxmp = (XMPSchemaPDFA) xmp.getSchemaByClass(XMPSchemaPDFA.class);
                if( pdfaxmp != null ) {
                    metadata.set("pdfaid:part", pdfaxmp.getPart());
                    metadata.set("pdfaid:conformance", pdfaxmp.getConformance());
                    String version = "A-"+pdfaxmp.getPart()+pdfaxmp.getConformance().toLowerCase();
                    //metadata.set("pdfa:version", version );                    
                    metadata.set("pdf:version", version );                    
                }
            }
            // Attempt to determine Adobe extension level:
            PdfDictionary extensions = reader.getCatalog().getAsDict(PdfName.EXTENSIONS);
            if( extensions != null ) {
                PdfDictionary adobeExt = extensions.getAsDict(PdfName.ADBE);
                if( adobeExt != null ) {
                    PdfName baseVersion = adobeExt.getAsName(PdfName.BASEVERSION);
                    int el = adobeExt.getAsNumber(PdfName.EXTENSIONLEVEL).intValue();
                    metadata.set("pdf:version", baseVersion.toString().substring(1)+" Adobe Extension Level "+el );
                }
            }
            // Ensure the normalised metadata are mapped in:
            if(  map.get( "Title" ) != null ) 
                metadata.set( Metadata.TITLE, map.get( "Title" ) );
            if(  map.get( "Author" ) != null ) 
                metadata.set( Metadata.AUTHOR, map.get( "Author" ) );
        } catch( Exception e ) {
            System.err.println( "PDFParser.extractMetadata() caught Exception: " + e.getMessage() );
            e.printStackTrace();
        }
    }
    
    private static String getCryptoModeAsString( PdfReader reader ) {
        int mode = reader.getCryptoMode();
        // TODO Make this into a more readable string, but tricky as it's a bitmask.
        // Need to use FLAG_A|FLAG_B output syntax, as strace does.
        // @see com.itextpdf.text.pdf.PDFEncryption.setCryptoMode(int mode, int kl);
        return ""+mode;
    }

    /**
     * @deprecated This method will be removed in Apache Tika 1.0.
     */
    public void parse( InputStream stream, ContentHandler handler, Metadata metadata ) throws IOException, SAXException, TikaException {
        parse( stream, handler, metadata, new ParseContext() );
    }
}
