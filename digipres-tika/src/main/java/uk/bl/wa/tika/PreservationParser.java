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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import uk.bl.wa.tika.parser.iso9660.ISO9660Parser;
import uk.bl.wa.tika.parser.warc.ARCParser;
import uk.bl.wa.tika.parser.warc.WARCParser;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class PreservationParser extends AutoDetectParser {
    private static Logger log = LoggerFactory.getLogger(PreservationParser.class.getName());
    
    public static final String EXT_MIME_TYPE = "Extended-MIME-Type";
    
    private boolean initialised = false;
    
    private NonRecursiveEmbeddedDocumentExtractor embedded = null;
    
    /**
     * 
     */
    private static final long serialVersionUID = 6809061887891839162L;
    
    /**
     * Modify the configuration as needed:
     * @param context 
     */
    public void init(ParseContext context) {
        if( this.initialised ) return;
        // Add the HighlightJS detector:
        /*
        CompositeDetector detect = (CompositeDetector) this.getDetector();
        List<Detector> detectors = detect.getDetectors();
        detectors.add( new HighlightJSDetector() );
        this.setDetector( new CompositeDetector(detectors));
        */
        // Override the built-in PDF parser (based on PDFBox) with our own (based in iText):
        MediaType pdf = MediaType.parse("application/pdf");
        Map<MediaType, Parser> parsers = getParsers();
        parsers.put( pdf, new uk.bl.wa.tika.parser.pdf.pdfbox.PDFParser() );
        parsers.put( MediaType.parse("application/x-iso9660-image"), new ISO9660Parser() );
        parsers.put( MediaType.parse("application/x-internet-archive"), new ARCParser() );
        parsers.put( MediaType.parse("application/warc"), new WARCParser() );
        setParsers(parsers);
        // Override the recursive parsing:
        embedded = new NonRecursiveEmbeddedDocumentExtractor(context);
        context.set( EmbeddedDocumentExtractor.class, embedded );
        this.initialised = true;
    }

    /* (non-Javadoc)
     * @see org.apache.tika.parser.AutoDetectParser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata, org.apache.tika.parser.ParseContext)
     */
    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
        
        // Override with custom parsers, etc.:
        if( !initialised ) init(context);

        // Pick up the detected MIME Type passed in from above:
        String providedType = metadata.get( Metadata.CONTENT_TYPE );
        log.info("Supplied hint, providedType = " + providedType);
        
        /* debug info
        String[] names = metadata.names();
        Arrays.sort(names);
        for( String name : names ) {
            System.out.println("PPPPre : "+name+" = "+metadata.get(name));
        }
         */

        // Parse:
        super.parse(stream, handler, metadata, context);
        
        /* Debug info
        names = metadata.names();
        Arrays.sort(names);
        for( String name : names ) {
            System.out.println("PPPPost : "+name+" = "+metadata.get(name));
        }
         */
        
        // Build the extended MIME Type, incorporating version and creator software etc.
        MediaType tikaType = null;
        try {
            if( providedType == null ) {
                tikaType = MediaType.parse( metadata.get( Metadata.CONTENT_TYPE ) );
            } else {
                tikaType = MediaType.parse( providedType );
            }
        } catch ( Exception e) {
            // Stop here and return if this failed:
            log.error("Could not parse MIME Type: "+metadata.get( Metadata.CONTENT_TYPE ));
            tikaType = MediaType.OCTET_STREAM;
            metadata.remove( Metadata.CONTENT_TYPE );
        }
        HashMap<String, String> hm = new HashMap<String,String>();
        
        // Content encoding, if any:
        String encoding = metadata.get( Metadata.CONTENT_ENCODING );
        /*
        if( encoding != null ) {
            if ( "text".equals( tikaType.getType() ) ) {
                tikaType.setParameter( "charset", encoding.toLowerCase() );
            } else {
                tikaType.setParameter( "encoding", encoding );
            }
        }
        */
        // Also look to record the software:
        String software = null;
        // Application ID, MS Office only AFAICT, and the VERSION is only doc
        if( metadata.get( Metadata.APPLICATION_NAME ) != null ) software = metadata.get( Metadata.APPLICATION_NAME );
        if( metadata.get( Metadata.APPLICATION_VERSION ) != null ) software += " "+metadata.get( Metadata.APPLICATION_VERSION);
        // Images, e.g. JPEG and TIFF, can have 'Software', 'tiff:Software',
        if( metadata.get("pdf:producer") != null ) software = metadata.get("pdf:producer");
        if( metadata.get( "Software" ) != null ) software = metadata.get( "Software" );
        if( metadata.get( Metadata.SOFTWARE ) != null ) software = metadata.get( Metadata.SOFTWARE );
        if( metadata.get( "generator" ) != null ) software = metadata.get( "generator" );
        // PNGs have a 'tEXt tEXtEntry: keyword=Software, value=GPL Ghostscript 8.71'
        String png_textentry = metadata.get("tEXt tEXtEntry");
        if( png_textentry != null && png_textentry.contains("keyword=Software, value=") )
            software = png_textentry.replace("keyword=Software, value=", "");
        /* Some JPEGs have this:
Jpeg Comment: CREATOR: gd-jpeg v1.0 (using IJG JPEG v62), default quality
comment: CREATOR: gd-jpeg v1.0 (using IJG JPEG v62), default quality
         */
        if( software != null ) {
            metadata.set(Metadata.SOFTWARE, software);
            hm.put("software", software);
        }
        // Also, if there is any trace of any hardware, record it here:
        if( metadata.get( Metadata.EQUIPMENT_MODEL ) != null )
            metadata.set("hardware", metadata.get( Metadata.EQUIPMENT_MODEL));
        
        // If there is any trace of the source document, add it here:
        if( metadata.get("pdf:creator") != null ) {
            hm.put("source",metadata.get("pdf:creator") );
        }
        
        // Fall back on special type for empty resources:
        if( "0".equals(metadata.get(Metadata.CONTENT_LENGTH)) ) {
            metadata.set(Metadata.CONTENT_TYPE, "application/x-empty");
        }
        
        // Version:
        if( metadata.get("pdf:version") != null ) {
            hm.put("version",metadata.get("pdf:version") );
        }
        
        // Return extended MIME Type:
        tikaType = new MediaType(tikaType, hm);
        metadata.set(EXT_MIME_TYPE, tikaType.toString());
        
        // Other sources of modification time?
        //md.get(Metadata.LAST_MODIFIED); //might be useful, as would any embedded version
        // e.g. a jpg with 'Date/Time: 2011:10:07 11:35:42'?
        // e.g. a png with
        // 'Document ImageModificationTime: year=2011, month=7, day=29, hour=15, minute=33, second=5'
        // 'tIME: year=2011, month=7, day=29, hour=15, minute=33, second=5'

    }
    
    /**
     * 
     * @param context
     * @param recurse
     */
    public void setRecursive( ParseContext context, boolean recurse ) {
        init(context);
        embedded.setParseEmbedded(recurse);
    }

}
