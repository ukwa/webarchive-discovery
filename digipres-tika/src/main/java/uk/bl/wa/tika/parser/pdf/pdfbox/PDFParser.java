/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.bl.wa.tika.parser.pdf.pdfbox;

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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.jempbox.xmp.XMPMetadata;
import org.apache.jempbox.xmp.XMPSchemaPDF;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSString;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.PasswordProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import uk.bl.wa.tika.parser.pdf.XMPSchemaPDFA;

/**
 * PDF parser.
 * <p>
 * This parser can process also encrypted PDF documents if the required
 * password is given as a part of the input metadata associated with a
 * document. If no password is given, then this parser will try decrypting
 * the document using the empty password that's often used with PDFs.
 */
public class PDFParser extends AbstractParser {
    
    private static Logger log = LoggerFactory.getLogger(PDFParser.class);

    /** Serial version UID */
    private static final long serialVersionUID = -752276948656079347L;

    // True if we let PDFBox "guess" where spaces should go:
    private boolean enableAutoSpace = true;

    // True if we let PDFBox remove duplicate overlapping text:
    private boolean suppressDuplicateOverlappingText;

    // True if we extract annotation text ourselves
    // (workaround for PDFBOX-1143):
    private boolean extractAnnotationText = true;

    // True if we should sort text tokens by position
    // (necessary for some PDFs, but messes up other PDFs):
    private boolean sortByPosition = false;

    /**
     * Metadata key for giving the document password to the parser.
     *
     * @since Apache Tika 0.5
     * @deprecated Supply a {@link PasswordProvider} on the {@link ParseContext} instead
     */
    public static final String PASSWORD = "org.apache.tika.parser.pdf.password";

    private static final Set<MediaType> SUPPORTED_TYPES =
        Collections.singleton(MediaType.application("pdf"));

    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

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
            pdfDocument = PDDocument.load(new CloseShieldInputStream(stream),
                    MemoryUsageSetting.setupMixed(100 * 1024 * 1024));
           
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
               
            }
            metadata.set(Metadata.CONTENT_TYPE, "application/pdf");
            extractMetadata(pdfDocument, metadata);
            PDF2XHTML.process(pdfDocument, handler, metadata,
                              extractAnnotationText, enableAutoSpace,
                              suppressDuplicateOverlappingText, sortByPosition);
        } catch( Exception e ) {
            log.error("Exception while parsing PDF: "+e);
        } finally {
            if (pdfDocument != null) {
               pdfDocument.close();
            }
            tmp.dispose();
        }
    }

    private void extractMetadata(PDDocument document, Metadata metadata)
            throws TikaException {
        PDDocumentInformation info = document.getDocumentInformation();
        metadata.set(PagedText.N_PAGES, document.getNumberOfPages());
        addMetadata(metadata, Metadata.TITLE, info.getTitle());
        addMetadata(metadata, Metadata.AUTHOR, info.getAuthor());
        addMetadata(metadata, Metadata.KEYWORDS, info.getKeywords());
        addMetadata(metadata, "pdf:creator", info.getCreator());
        addMetadata(metadata, "pdf:producer", info.getProducer());
        addMetadata(metadata, Metadata.SUBJECT, info.getSubject());
        addMetadata(metadata, "trapped", info.getTrapped());
        addMetadata(metadata, "created", info.getCreationDate());
        addMetadata(metadata, Metadata.CREATION_DATE, info.getCreationDate());
        Calendar modified = info.getModificationDate();
        addMetadata(metadata, Metadata.LAST_MODIFIED, modified);
        
        // All remaining metadata is custom
        // Copy this over as-is
        List<String> handledMetadata = Arrays.asList(new String[] {
             "Author", "Creator", "CreationDate", "ModDate",
             "Keywords", "Producer", "Subject", "Title", "Trapped"
        });
        if (info.getCOSObject() != null
                && info.getCOSObject().keySet() != null) {
            for (COSName key : info.getCOSObject().keySet()) {
            String name = key.getName();
            if(! handledMetadata.contains(name)) {
                    addMetadata(metadata, name,
                            info.getCOSObject().getDictionaryObject(key));
            }
          }
        }
        // ANJ Extensions:
        //
        //
        // Add other data of interest:
        metadata.set("pdf:version", ""+document.getDocument().getVersion());
        metadata.set("pdf:numPages", ""+document.getNumberOfPages());
        //metadata.set("pdf:cryptoMode", ""+getCryptoModeAsString(reader));
        //metadata.set("pdf:openedWithFullPermissions", ""+reader.isOpenedWithFullPermissions());
        metadata.set("pdf:encrypted", ""+document.isEncrypted());
        //metadata.set("pdf:metadataEncrypted", ""+document.isMetadataEncrypted());
        //metadata.set("pdf:128key", ""+reader.is128Key());
        //metadata.set("pdf:tampered", ""+reader.isTampered());
        try {
            if( document.getDocumentCatalog().getMetadata() != null ) {
                XMPMetadata xmp = XMPMetadata.load(document.getDocumentCatalog()
                        .getMetadata().exportXMPMetadata());
                // There is a special class for grabbing data in the PDF schema - not sure it will add much here:
                // Could parse xmp:CreatorTool and pdf:Producer etc. etc. out of here.
                XMPSchemaPDF pdfxmp = xmp.getPDFSchema();
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
                // TODO WARN if this XMP version is inconsistent with document header version?
            }
        } catch (IOException e) {
            log.error("XMP Parsing failed: "+e);
            metadata.set("pdf:metadata-xmp-parse-failed", ""+e);
        }
        
        // Attempt to determine Adobe extension level, if present:
        COSDictionary root = document.getDocumentCatalog().getCOSObject();
        COSDictionary extensions = (COSDictionary) root.getDictionaryObject(COSName.getPDFName("Extensions") );
        if( extensions != null ) {
            for( COSName extName : extensions.keySet() ) {
                // If it's an Adobe one, interpret it to determine the extension level:
                if( extName.equals( COSName.getPDFName("ADBE") )) {
                    COSDictionary adobeExt = (COSDictionary) extensions.getDictionaryObject(extName);
                    if( adobeExt != null ){
                        String baseVersion = adobeExt.getNameAsString(COSName.getPDFName("BaseVersion"));
                        int el = adobeExt.getInt(COSName.getPDFName("ExtensionLevel"));
                        metadata.set("pdf:version", baseVersion+" Adobe Extension Level "+el );
                    }
                    // TODO WARN if this embedded version is inconsistent with document header version?
                } else {
                    // WARN that there is an Extension, but it's not Adobe's, and so is a 'new' format'.
                    metadata.set("pdf:foundNonAdobeExtensionName", extName.getName());
                }
            }
        }
        // End Of ANJ Extensions.
    }

    private void addMetadata(Metadata metadata, String name, String value) {
        if (value != null) {
            metadata.add(name, value);
        }
    }

    private void addMetadata(Metadata metadata, String name, Calendar value) {
        if (value != null) {
            metadata.set(name, value.getTime().toString());
        }
    }

    private void addMetadata(Metadata metadata, Property property, Calendar value) {
        if (value != null) {
            metadata.set(property, value.getTime());
        }
    }

    /**
     * Used when processing custom metadata entries, as PDFBox won't do
     *  the conversion for us in the way it does for the standard ones
     */
    private void addMetadata(Metadata metadata, String name, COSBase value) {
        if(value instanceof COSArray) {
            for(Object v : ((COSArray)value).toList()) {
                addMetadata(metadata, name, (COSBase)v);
            }
        } else if(value instanceof COSString) {
            addMetadata(metadata, name, ((COSString)value).getString());
        } else {
            addMetadata(metadata, name, value.toString());
        }
    }

    /**
     *  If true (the default), the parser should estimate
     *  where spaces should be inserted between words.  For
     *  many PDFs this is necessary as they do not include
     *  explicit whitespace characters.
     */
    public void setEnableAutoSpace(boolean v) {
        enableAutoSpace = v;
    }

    /** @see #setEnableAutoSpace. */
    public boolean getEnableAutoSpace() {
        return enableAutoSpace;
    }

    /**
     * If true (the default), text in annotations will be
     * extracted.
     */
    public void setExtractAnnotationText(boolean v) {
        extractAnnotationText = v;
    }

    /**
     * If true, text in annotations will be extracted.
     */
    public boolean getExtractAnnotationText() {
        return extractAnnotationText;
    }

    /**
     *  If true, the parser should try to remove duplicated
     *  text over the same region.  This is needed for some
     *  PDFs that achieve bolding by re-writing the same
     *  text in the same area.  Note that this can
     *  slow down extraction substantially (PDFBOX-956) and
     *  sometimes remove characters that were not in fact
     *  duplicated (PDFBOX-1155).  By default this is disabled.
     */
    public void setSuppressDuplicateOverlappingText(boolean v) {
        suppressDuplicateOverlappingText = v;
    }

    /** @see #setSuppressDuplicateOverlappingText. */
    public boolean getSuppressDuplicateOverlappingText() {
        return suppressDuplicateOverlappingText;
    }

    /**
     *  If true, sort text tokens by their x/y position
     *  before extracting text.  This may be necessary for
     *  some PDFs (if the text tokens are not rendered "in
     *  order"), while for other PDFs it can produce the
     *  wrong result (for example if there are 2 columns,
     *  the text will be interleaved).  Default is false.
     */
    public void setSortByPosition(boolean v) {
        sortByPosition = v;
    }

    /** @see #setSortByPosition. */
    public boolean getSortByPosition() {
        return sortByPosition;
    }

    public static void main( String[] args ) {
        try {
            FileInputStream input = new FileInputStream( new File( "src/test/resources/jap_91055688_japredcross_ss_ue_fnl_12212011.pdf"));//simple-PDFA-1a.pdf" ) );
            OutputStream output = System.out; //new FileOutputStream( new File( "Z:/part-00001.xml" ) );

            Metadata metadata = new Metadata();
            PDFParser parser = new PDFParser();
            parser.parse(input, new DefaultHandler() , metadata, new ParseContext() );
            input.close();
            
            for( String key : metadata.names() ) {
                output.write( (key+" : "+metadata.get(key)+"\n").getBytes( "UTF-8" ) );
            }
            output.close();
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }}
