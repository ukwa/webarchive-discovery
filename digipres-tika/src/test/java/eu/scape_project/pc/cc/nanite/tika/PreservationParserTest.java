/**
 * 
 */
package eu.scape_project.pc.cc.nanite.tika;

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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import javax.activation.MimeTypeParseException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import uk.bl.wa.tika.PreservationParser;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class PreservationParserTest {
    
    CompositeParser parser = null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        parser = new PreservationParser();
    }

    /**
     * Test method for {@link uk.bl.wa.tika.PreservationParser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata, org.apache.tika.parser.ParseContext)}.
     * @throws TikaException 
     * @throws SAXException 
     * @throws IOException 
     * @throws MimeTypeParseException 
     */
    @Test
    public void testParseInputStreamContentHandlerMetadataParseContext() throws IOException, SAXException, TikaException, MimeTypeParseException {
        // PDFs
        this.testExtendedMIMEType( "/simple.pdf", 
                "application/pdf; version=\"1.4\"; software=\"OpenOffice.org 3.2\"; source=Writer");
        this.testExtendedMIMEType( "/simple-PDFA-1a.pdf", 
                "application/pdf; version=\"A-1a\"; software=\"OpenOffice.org 3.2\"; source=Writer");
        this.testExtendedMIMEType( "/simple-password-nocopy.pdf", 
                "application/pdf; version=\"1.4\"; software=\"OpenOffice.org 3.2\"; source=Writer");
        // ODT
        this.testExtendedMIMEType( "/simple.odt", 
                "application/vnd.oasis.opendocument.text; software=\"OpenOffice.org\\/3.2$Win32 OpenOffice.org_project\\/320m12$Build-9483\"");
        // PNG
        //this.testExtendedMIMEType( "/Users/andy/Documents/workspace/nanite/nanite-tika/src/test/resources/variatio-ipsius/apple-pages-09-4.1-923/convert-6.7.5-7/test.png", 
        //        "image/png; software=\"ImageMagick\"");
        // JPEG
        //this.testExtendedMIMEType( "/Users/andy/Documents/workspace/tika/tika-parsers/src/test/resources/test-documents/testJPEG_EXIF.jpg", 
        //        "image/jpeg; software=\"Adobe Photoshop CS3 Macintosh\"; hardware=\"Canon EOS 40D\"");
        // TIFF?
    }
    
    /**
     * 
     * @param filename
     * @param expected
     * @throws TikaException 
     * @throws SAXException 
     * @throws IOException 
     * @throws MimeTypeParseException 
     */
    private void testExtendedMIMEType(String filename, String expected ) throws IOException, SAXException, TikaException, MimeTypeParseException {
        // Get the source file off the TEST class path:
        InputStream input = getClass().getResourceAsStream(filename);
        
        Metadata metadata = new Metadata();
        parser.parse(input, new DefaultHandler() , metadata, new ParseContext() );
        input.close();
        
        // Report all metadata, for interest
        System.out.println("Metadata for: "+filename);
        String[] names = metadata.names();
        Arrays.sort(names);
        for( String key : names ) {
            System.out.println( (key+" : "+metadata.get(key)) );
        }
        System.out.println("----");

        // Recover the extended MIME Type:
        MediaType tikaType = MediaType.parse(metadata.get(PreservationParser.EXT_MIME_TYPE) );
        // Assert equality:
        assertEquals( MediaType.parse( expected ), tikaType );

}

}
