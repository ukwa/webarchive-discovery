/**
 * 
 */
package uk.bl.wa.parsers;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.pdfbox.preflight.PreflightDocument;
import org.apache.pdfbox.preflight.ValidationResult;
import org.apache.pdfbox.preflight.ValidationResult.ValidationError;
import org.apache.pdfbox.preflight.exception.SyntaxValidationException;
import org.apache.pdfbox.preflight.parser.PreflightParser;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import uk.bl.wa.util.InputStreamDataSource;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ApachePreflightParser extends AbstractParser {

    /** */
    private static final long serialVersionUID = 710873621129254338L;

    /** */
    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(
                  MediaType.application("pdf")
            )));

    public static final Property PDF_PREFLIGHT_VALID = Property.internalBoolean("PDF-A-PREFLIGHT-VALID");

    public static final Property PDF_PREFLIGHT_ERRORS = Property.internalTextBag("PDF-A-PREFLIGHT-ERRORS");

    
    
    /* (non-Javadoc)
     * @see org.apache.tika.parser.Parser#getSupportedTypes(org.apache.tika.parser.ParseContext)
     */
    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    /* (non-Javadoc)
     * @see org.apache.tika.parser.Parser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata, org.apache.tika.parser.ParseContext)
     */
    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
        
        // Attempt to reduce logging of stacktraces:
        //System.setProperty("log4j.logger.org.apache.pdfbox","");

        // Set up the validation result:
        ValidationResult result = null;

        InputStreamDataSource isds = new InputStreamDataSource(stream);
        PreflightParser parser = new PreflightParser(isds);
        PreflightDocument document = null;
        try {

          /* Parse the PDF file with PreflightParser that inherits from the NonSequentialParser.
           * Some additional controls are present to check a set of PDF/A requirements. 
           * (Stream length consistency, EOL after some Keyword...)
           */
            parser.parse();

          /* Once the syntax validation is done, 
           * the parser can provide a PreflightDocument 
           * (that inherits from PDDocument) 
           * This document process the end of PDF/A validation.
           */
            document = parser.getPreflightDocument();
            document.validate();
          
            // Get validation result
            result = document.getResult();

        } catch (SyntaxValidationException e) {
            /*
             * the parse method can throw a SyntaxValidationExceptionif the PDF
             * file can't be parsed.
             * 
             * In this case, the exception contains an instance of
             * ValidationResult
             */
            result = e.getResult();
        } catch (Exception e) {
            // Otherwise, a NULL result:
            result = null;

        } finally {
            // Ensure the document is always closed:
            if (document != null)
                document.close();
        }

        // display validation result
        Set<String> rs = new HashSet<String>();
        if (result != null && result.isValid()) {
          //System.out.println("The resource is not a valid PDF/A-1b file");
          metadata.set( PDF_PREFLIGHT_VALID, Boolean.TRUE.toString() );
        } else {
          //System.out.println("The resource is not valid, error(s) :");
          metadata.set( PDF_PREFLIGHT_VALID, Boolean.FALSE.toString() );
            if (result != null) {
                for (ValidationError error : result.getErrorsList()) {
                    // System.out.println(error.getErrorCode() + " : " +
                    // error.getDetails());
                    rs.add(error.getErrorCode() + " : " + error.getDetails());
                }
            }
        }

        metadata.set( PDF_PREFLIGHT_ERRORS , rs.toArray( new String[] {} ));

    }

}
