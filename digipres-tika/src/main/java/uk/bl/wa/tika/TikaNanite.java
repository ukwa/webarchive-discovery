/**
 * 
 */
package uk.bl.wa.tika;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import uk.bl.wa.tika.parser.pdf.itext.PDFParser;

/**
 * Base on http://wiki.apache.org/tika/RecursiveMetadata
 * 
 * @author AnJackson
 * 
 */
public class TikaNanite {

    public static void main(String[] args) throws Exception {
        
        PreservationParser parser = new PreservationParser();
        // Wrap it in a recursive parser, to access the metadata.
        Parser recursiveReportingParser = new RecursiveMetadataParser(parser);
        // Set up the context:
        ParseContext context = new ParseContext();
        context.set(Parser.class, recursiveReportingParser);
        parser.init(context);
        // Control recursion:
        //parser.setRecursive(context, false);

        // Basic handler (ignores/pass-through-in-silence):
        //ContentHandler handler = new DefaultHandler();
        // Abort handler, limiting the output size, to avoid OOM:
        ContentHandler handler = new WriteOutContentHandler(1000*1024);
        
        for( String arg : args ) {
            File inputFile = new File(arg);
        Metadata metadata = new Metadata();
        metadata.add( Metadata.RESOURCE_NAME_KEY, inputFile.toURI().toString());
        // Instream
        InputStream stream = TikaInputStream.get(inputFile);
        // Detect
        //Tika tika = new Tika();
        MediaType type = parser.getDetector().detect(stream, metadata);
        System.out.println("Detector found: "+type);
        metadata.add( Metadata.CONTENT_TYPE, type.toString());
        // Parse
        try {
            recursiveReportingParser.parse(stream, handler, metadata, context);
        } catch (Exception e ) {
            System.out.println("---- Exception: "+e);            
        } finally {
            stream.close();
        }
        
        System.out.println("--EOF-- Top Level Metadata --");
        String[] names = metadata.names();
        Arrays.sort(names);
        for( String name : names ) {
            System.out.println("MD: "+name+" = "+metadata.get(name));
        }
        System.out.println("----");
        }
    }

    /**
     * For this to work reliably, we will need to modify PackageExtractor
     * so that the parent-child relationship is maintained. Otherwise, 
     * the identity of files gets confused when there are ZIPs in ZIPs etc.
     * 
     * @author AnJackson
     */
    private static class RecursiveMetadataParser extends ParserDecorator {

        /** */
        private static final long serialVersionUID = 5133646719357986442L;

        public RecursiveMetadataParser(Parser parser) {
            super(parser);
        }

        @Override
        public void parse(InputStream stream, ContentHandler ignore,
                Metadata metadata, ParseContext context) throws IOException,
                SAXException, TikaException {
            
            System.out.println("----");
            String providedType = metadata.get( Metadata.CONTENT_TYPE );
            System.out.println("Pre-parse Content-Type = " + providedType);

            try {
                super.parse(stream, ignore, metadata, context);
            } catch (Exception e ) {
                System.out.println("---- Exception: "+e);
                e.printStackTrace();
            }
            
            System.out.println("----");
            System.out.println("resourceName = "+metadata.get(Metadata.RESOURCE_NAME_KEY));
            System.out.println("----");
            String[] names = metadata.names();
            Arrays.sort(names);
            for( String name : names ) {
                System.out.println("RMD : "+name+" = "+metadata.get(name));
            }
            System.out.println("----");
            String text = ignore.toString();
            if( text.length() > 10 ) text = text.substring(0,10);
            //System.out.println(text);
        }
    }

}
