/**
 * 
 */
package uk.bl.wa.tika.parser.pdf;

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

import org.apache.pdfbox.io.RandomAccess;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.PasswordProvider;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class PDFParser extends org.apache.tika.parser.pdf.PDFParser {

    /**
     * 
     */
    private static final long serialVersionUID = -746530738295311633L;

    /*
    public void parse(
            InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
       
        PDDocument pdfDocument = null;
        TemporaryResources tmp = new TemporaryResources();

        try {
            // PDFBox can process entirely in memory, or can use a temp file
            //  for unpacked / processed resources
            // Decide which to do based on if we're reading from a file or not already
            TikaInputStream tstream = TikaInputStream.cast(stream);
            if (tstream != null && tstream.hasFile()) {
               // File based, take that as a cue to use a temporary file
               RandomAccess scratchFile = new RandomAccessFile(tmp.createTemporaryFile(), "rw");
               pdfDocument = PDDocument.load(new CloseShieldInputStream(stream), scratchFile, true);
            } else {
               // Go for the normal, stream based in-memory parsing
               pdfDocument = PDDocument.load(new CloseShieldInputStream(stream), true);
            }
           
            if (pdfDocument.isEncrypted()) {
                String password = null;
                
                // Did they supply a new style Password Provider?
                PasswordProvider passwordProvider = context.get(PasswordProvider.class);
                if (passwordProvider != null) {
                   password = passwordProvider.getPassword(metadata);
                }
                
                // Fall back on the old style metadata if set
                if (password == null && metadata.get(PASSWORD) != null) {
                   password = metadata.get(PASSWORD);
                }
                
                // If no password is given, use an empty string as the default
                if (password == null) {
                   password = "";
                }
               
                try {
                    pdfDocument.decrypt(password);
                } catch (Exception e) {
                    // Ignore
                }
            }
            metadata.set(Metadata.CONTENT_TYPE, "application/pdf");
            extractMetadata(pdfDocument, metadata);
            PDF2XHTML.process(pdfDocument, handler, metadata,
                              extractAnnotationText, enableAutoSpace,
                              suppressDuplicateOverlappingText, sortByPosition);
        } finally {
            if (pdfDocument != null) {
               pdfDocument.close();
            }
            tmp.dispose();
        }
    }
    */
    
}
