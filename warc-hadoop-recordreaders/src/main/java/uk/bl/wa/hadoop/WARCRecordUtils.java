package uk.bl.wa.hadoop;

/*
 * #%L
 * warc-hadoop-recordreaders
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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

public class WARCRecordUtils {
    
    public static InputStream getPayload(ArchiveRecord record) throws IOException {
        if( record instanceof ARCRecord ) return getPayload( (ARCRecord) record );
        if( record instanceof WARCRecord ) return getPayload( (WARCRecord) record );
        return null;
    }
    
    private static BufferedInputStream getPayload( WARCRecord record ) throws IOException {
        WARCRecordUtils.getHeaders( record, true );
        return new BufferedInputStream( record );
    }

    private static BufferedInputStream getPayload( ARCRecord record ) throws IOException {
        return new BufferedInputStream( record );
    }

    public static String getHeaders( InputStream input, boolean isBlock ) {
        StringBuilder headers = new StringBuilder();
        String line;
        try {
            line = WARCRecordUtils.readLine( input );
            if( isBlock && line.indexOf( "HTTP" ) != 0 ) {
                return headers.toString();
            }
            while( !line.matches( "^\\s*$" ) ) {
                headers.append( line );
                line = WARCRecordUtils.readLine( input );
            }
            headers.append( line );
        } catch( IOException e ) {
            System.err.println( "getHeaders(): " + e.toString() );
        }
        return headers.toString();
    }
    
    public static String readLine( InputStream inputstream ) throws IOException {
        int chr;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while( ( chr = inputstream.read() ) >= 0 ) {
            buffer.write( chr );
            if( chr == '\n' ) {
                break;
            }
        }
        return new String( buffer.toByteArray() );
    }

}
