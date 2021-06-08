/**
 * 
 */
package uk.bl.wa.tika.parser.iso9660;

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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import net.didion.loopy.iso9660.ISO9660FileEntry;
import net.didion.loopy.iso9660.ISO9660FileSystem;
import net.didion.loopy.iso9660.ISO9660VolumeDescriptorSet;

import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ISO9660Extractor {

    private final ContentHandler handler;

    private final Metadata metadata;

    private final EmbeddedDocumentExtractor extractor;

    public ISO9660Extractor(
            ContentHandler handler, Metadata metadata, ParseContext context) {
        this.handler = handler;
        this.metadata = metadata;

        EmbeddedDocumentExtractor ex = context.get(EmbeddedDocumentExtractor.class);

        if (ex==null) {
            this.extractor = new ParsingEmbeddedDocumentExtractor(context);
        } else {
            this.extractor = ex;
        }

    }

    /**
     * Extend the ISO9660 class to expose the Volume information.
     * @author Andrew Jackson <Andrew.Jackson@bl.uk>
     */
    public class ISO9660FS extends ISO9660FileSystem {

        public ISO9660FS(File file, boolean readOnly) throws IOException {
            super(file, readOnly);
        }

        public ISO9660VolumeDescriptorSet getVolumeDescriptorSet() {
            if( super.getVolumeDescriptorSet() == null ) {
                try {
                    super.loadVolumeDescriptors();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            return (ISO9660VolumeDescriptorSet) super.getVolumeDescriptorSet();
        }
    }


    /* (non-Javadoc)
     * @see org.apache.tika.parser.AbstractParser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata)
     */
    //@Override
    public void parse(InputStream stream) throws IOException, SAXException, TikaException {
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        
        /* Use the loopy Java ISO9660 classes to retrieve the (possibly)
         * compressed entries as individual source units.
         */
        TemporaryResources tmp = new TemporaryResources();
        File file = tmp.createTemporaryFile();
        OutputStream out = new FileOutputStream(file);
        try {
            IOUtils.copy(stream, out);
        } finally {
            out.close();
        }

        ISO9660FS iso = new ISO9660FS(file, true);
        ISO9660VolumeDescriptorSet vds = iso.getVolumeDescriptorSet();
        metadata.set("iso:volumeIdentifier",vds.getVolumeIdentifier());
        metadata.set("iso:standardIdentifier",vds.getStandardIdentifier());
        metadata.set("iso:systemIdentifier",vds.getSystemIdentifier());
        metadata.set("iso:volumeSetIdentifier",vds.getVolumeSetIdentifier());
        metadata.set("iso:publisher",vds.getPublisher());
        metadata.set("iso:preparer",vds.getPreparer());
        metadata.set("iso:encoding",vds.getEncoding());
        if (iso != null) {

            try {
                /* ISO9660 entries (files and directories) are not necessarily in
                 * hierarchical order.  Also, directories may be implicit, that
                 * is, referred to in the pathnames of files or directories but
                 * not explicitly present in the form of a directory entry.
                 * 
                 * Since all files and directories need to be associated with 
                 * the correct parent directory in order for aggregate
                 * characterization to work properly, we there are three stages
                 * of processing:
                 * 
                 * (1) Identify all explicit directory entries, creating
                 *     Directory sources and putting them into a map keyed to
                 *     the directory pathname.
                 *     
                 * (2) Identify all implicit directories (by extracting
                 *     directories from pathnames and checking to see if they
                 *     are not already in the map), creating Directory sources
                 *     and putting them into the map.  Also characterize any
                 *     top-level file entries (children of the ISO9660 file) that
                 *     are found.
                 *     
                 * (3) Directly characterize all top-level directories, that
                 *     is, those whose parent is the ISO9660 file.  This will
                 *     implicitly characterize all child files and directories.
                 */
                List<String> map = new ArrayList<String>();
                Enumeration<ISO9660FileEntry> en = iso.getEntries();

                /* (1) Identify all directories that are explicit entries. */ 
                while (en.hasMoreElements()) {
                    ISO9660FileEntry entry = en.nextElement();
                    String name = this.getFullPath(entry);
                    System.out.println("ISO9660 - Pre-scan found directory named: "+name);
                    /* Delete trailing slash from path name, if necessary. Although this
                     * always should be a forward slash (/), in practice a backward slash
                     * \) may be found.
                     */
                    int in = name.lastIndexOf('/');
                    if (in < 0) {
                        in = name.lastIndexOf('\\');
                    }
                    if (in == name.length() - 1) {
                        name = name.substring(0, in);
                    }
                    //                        Source src =
                    //                            factory.getSource(jhove2, ISO, entry);
                    /* Get the entry-specific properties. */
                    /*
                            long crc = entry.getCrc();
                            Digest crc32 = new Digest(AbstractArrayDigester.toHexString(crc),
                                                      CRC32Digester.ALGORITHM);
                            ISO9660EntryProperties properties =
                                new ISO9660EntryProperties(name, entry.getCompressedSize(), crc32,
                                                       entry.getComment(),
                                                       new Date(entry.getTime()));
                            src = src.addExtraProperties(properties);
                     */
                    String key = getFullPath(entry);
                    /* Remove trailing slash. Although this always
                     * should be a forward slash (/), in practice a
                     * backward slash (\) may be found. */
                    int len = key.length() - 1;
                    char ch = key.charAt(len);
                    if (ch == '/') {
                        key = key.substring(0, len);
                    }
                    else if (ch == '\\') {
                        key = key.substring(0, len);
                    }
                    // Now parse it...
                    if( entry.isDirectory() ) {
                        // FIXME What to do with directories?
                        System.out.println("ISO9660 - Found directory named: "+name+" "+entry.getPath());
                    } else {
                        // FIXME Parse the embedded file: 
                        System.out.println("ISO9660 - Found file named: "+name+" "+entry.getPath());
                        InputStream entryStream = iso.getInputStream(entry);
                        /* Get the entry-specific properties. */
                        /*
                                long crc = entry.getCrc();
                                Digest crc32 = new Digest(AbstractArrayDigester.toHexString(crc),
                                        CRC32Digester.ALGORITHM);
                                ISO9660EntryProperties properties =
                                    new ISO9660EntryProperties(name, entry.getCompressedSize(), crc32,
                                            entry.getComment(),
                                            new Date(entry.getTime()));
                         */
                        // Setup
                        Metadata entrydata = new Metadata();
                        entrydata.set(Metadata.RESOURCE_NAME_KEY, key);
                        // Use the delegate parser to parse the compressed document
                        if (extractor.shouldParseEmbedded(entrydata)) {
                            extractor.parseEmbedded(stream, xhtml, entrydata, true);
                        }
                    }
                }

            }  
            finally {
                iso.close();
                tmp.close();
            }
        }
        xhtml.endDocument();
    }

    /**
     * Helper to patch a consistent path from the ISO9660 Entry:
     * @param entry
     * @return
     */
    private String getFullPath(ISO9660FileEntry entry) {
        String fullPath = entry.getPath();
        if( fullPath == null || fullPath.length() == 0 ) fullPath = entry.getName();
        if( fullPath.charAt(0) != '.' ) fullPath = "./"+fullPath;
        return fullPath;
    }

}
