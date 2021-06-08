/**
 * 
 */
package uk.bl.wap.tika.parser.pdf.pdfbox;

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
import static org.junit.Assert.fail;

import java.io.InputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.junit.Test;
import org.xml.sax.helpers.DefaultHandler;

import uk.bl.wa.tika.parser.pdf.pdfbox.PDFParser;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class PDFParserTest {

    /**
     * Test method for {@link uk.bl.wa.tika.parser.pdf.pdfbox.PDFParser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata, org.apache.tika.parser.ParseContext)}.
     */
    @Test
    public void testParseInputStreamContentHandlerMetadataParseContext() {
        try {
            //FileInputStream input = new FileInputStream( new File( "src/test/resources/jap_91055688_japredcross_ss_ue_fnl_12212011.pdf"));//simple-PDFA-1a.pdf" ) );
            InputStream input = getClass().getResourceAsStream("/simple-PDFA-1a.pdf");
            
            Metadata metadata = new Metadata();
            PDFParser parser = new PDFParser();
            parser.parse((input), new DefaultHandler() , metadata, new ParseContext() );
            input.close();
            
            for( String key : metadata.names() ) {
                System.out.write((key + " : " + metadata.get(key) + "\n")
                        .getBytes("UTF-8"));
            }
            
        } catch( Exception e ) {
            e.printStackTrace();
            fail("Exception during parse: "+e);
        }
    }
}
