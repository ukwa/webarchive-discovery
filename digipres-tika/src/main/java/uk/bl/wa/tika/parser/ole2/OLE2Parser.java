/**
 * 
 */
package uk.bl.wa.tika.parser.ole2;

/*-
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Set;

import org.apache.poi.hpsf.ClassID;
import org.apache.poi.hpsf.NoPropertySetStreamException;
import org.apache.poi.hpsf.Property;
import org.apache.poi.hpsf.PropertySet;
import org.apache.poi.hpsf.Section;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.model.Ffn;
import org.apache.poi.hwpf.model.TextPiece;
import org.apache.poi.poifs.eventfilesystem.POIFSReader;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderEvent;
import org.apache.poi.poifs.eventfilesystem.POIFSReaderListener;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.DocumentNode;
import org.apache.poi.poifs.filesystem.Entry;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * @author andy
 *
 */
public class OLE2Parser extends AbstractParser {
    
    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
        
        HWPFDocument doc = new HWPFDocument (stream);
        System.out.println("ApplicationName: "+doc.getSummaryInformation().getApplicationName());
        System.out.println("OSVersion: "+doc.getSummaryInformation().getOSVersion());
        System.out.println("# paragraphs: "+doc.getDocumentSummaryInformation().getParCount());
        System.out.println("# bytes: "+doc.getDocumentSummaryInformation().getByteCount());
        System.out.println("# hidden: "+doc.getDocumentSummaryInformation().getHiddenCount());
        System.out.println("# lines: "+doc.getDocumentSummaryInformation().getLineCount());
        System.out.println("# mmclips: "+doc.getDocumentSummaryInformation().getMMClipCount());
        System.out.println("# notes: "+doc.getDocumentSummaryInformation().getNoteCount());
        System.out.println("# sections: "+doc.getDocumentSummaryInformation().getSectionCount());
        System.out.println("# slides: "+doc.getDocumentSummaryInformation().getSlideCount());
        System.out.println("format: "+doc.getDocumentSummaryInformation().getFormat());
        for( TextPiece tp : doc.getTextTable().getTextPieces() ) {
            System.out.println("TP: "+tp.getStringBuffer().substring(0, 100));
            System.out.println("TP: "+tp.getPieceDescriptor().isUnicode());
        }
        for( Object os : doc.getDocumentSummaryInformation().getSections() ) {
            Section s = (Section) os;
            System.out.println("ss# fid: "+s.getFormatID());
            System.out.println("ss# codepage: "+s.getCodepage());                
            System.out.println("ss# # properties: "+s.getPropertyCount());
            for( Property sp : s.getProperties() ) {
                System.out.println("ss# property: "+sp.getValue().getClass().getCanonicalName()+" "+sp.getValue());
            }
        }
        for( Ffn f : doc.getFontTable().getFontNames() ) {
            System.out.println("Font: "+f.getMainFontName()+", "+f.getSize()+", "+f.getWeight());
        }
        parseCompObj( stream );

        // This
        POIFSFileSystem fs = new POIFSFileSystem(stream);

        DirectoryEntry root = fs.getRoot();
        
        dump(root);
        
    }

    
    public static void parseCompObj(InputStream file) {
        Collector collector = new Collector();
        POIFSReader poifsReader = new POIFSReader();
        poifsReader.registerListener(collector, "\001CompObj");
        try {
            poifsReader.read(file);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // collector.classId now contains the result.

    }
    
    // http://mail-archives.apache.org/mod_mbox/poi-user/200504.mbox/%3C0IFL00BM77MPGW@mta6.srv.hcvlny.cv.net%3E
    // For CLSIDs:
    // http://anoochit.fedorapeople.org/rpmbuild/BUILD/msttcorefonts/cab-contents/wviewer.stf
    // http://www.msfn.org/board/topic/139093-create-standalone-word-97/
    // For CompObj format, not clear:
    // FlashPix Format Spec!
    
    public static class Collector implements POIFSReaderListener {
        private ClassID classId;

        public void processPOIFSReaderEvent(POIFSReaderEvent event) {
            InputStream stream = event.getStream();

            try {
                if (stream.skip(12) == 12) { // magic number for the offset to the clsid.
                    byte[] classIdBytes = new byte[ClassID.LENGTH];
                    if (stream.read(classIdBytes) == ClassID.LENGTH) {
                        classId = new ClassID(classIdBytes, 0);
                    }
                }
            } catch (IOException e) {
                // Handle error.
            }
            System.out.println("Found ClassID: "+classId);
        }
    }        
    

    public static void dump(DirectoryEntry root) throws IOException {
        System.out.println(root.getName()+" : storage CLSID "+root.getStorageClsid());
        for(Iterator it = root.getEntries(); it.hasNext();){
            Entry entry = (Entry)it.next();
            if(entry instanceof DocumentNode){
                DocumentNode node = (DocumentNode)entry;
                System.out.println("Node name: "+node.getName());
                System.out.println("Node desc: "+node.getShortDescription());
                System.out.println("Node size: "+node.getSize());
                DocumentInputStream is = new DocumentInputStream(node);
                
                try {
                    PropertySet ps = new PropertySet(is);
                    if( ps.getSectionCount() != 0 ) {
                        for( Property p : ps.getProperties() ) {
                            System.out.println("Prop: "+p.getID()+" "+p.getValue());
                        }
                    }
                } catch (NoPropertySetStreamException e) {
                    // TODO Auto-generated catch block
                    //e.printStackTrace();
                }
                //byte[] bytes = new byte[node.getSize()];
                //is.read(bytes);
                //is.close();

                //FileOutputStream out = new FileOutputStream(new File(parent, node.getName().trim()));
                //out.write(bytes);
                //out.close();
                //System.out.println("Node: "+new String(bytes).substring(0, 10));
            } else if (entry instanceof DirectoryEntry){
                DirectoryEntry dir = (DirectoryEntry)entry;
                dump(dir);
            } else {
                System.err.println("Skipping unsupported POIFS entry: " + entry);
            }
        }
    }


}
