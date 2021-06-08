/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.warcstats;

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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.url.UsableURI;
import org.archive.url.UsableURIFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.WritableArchiveRecord;

/**
 * @author Andrew.Jackson@bl.uk
 *
 */
public class WARCStatsMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, Text> {

    private static Logger log = LoggerFactory.getLogger(WARCStatsMapper.class);

    @Override
    public void map(Text key, WritableArchiveRecord value,
            OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        ArchiveRecord record = value.getRecord();
        ArchiveRecordHeader header = record.getHeader();

        // Logging for debug info:
        log.debug("Processing @"+header.getOffset()+
                "+"+record.available()+","+header.getLength()+
                ": "+header.getUrl());        
        for( String h : header.getHeaderFields().keySet()) {
            log.debug("ArchiveHeader: "+h+" -> "+header.getHeaderValue(h));
        }
        
        // count all records:
        output.collect( new Text("record-total"), new Text("RECORD-TOTAL"));
        // check type:
        output.collect( new Text("record-type"), new Text("WARC-RECORD-TYPE\t"+header.getHeaderValue( HEADER_KEY_TYPE )));
        if( record instanceof WARCRecord ) {
            output.collect( new Text("record-type"), new Text("RECORD-TYPE-WARC") );            
        } else if( record instanceof ARCRecord ) {
            output.collect( new Text("record-type"), new Text("RECORD-TYPE-ARC") );            
        } else {
            output.collect( new Text("record-type"), new Text("RECORD-TYPE-UNKNOWN") );
        }
        
        // Other stats:
        output.collect( new Text("content-types"), new Text("CONTENT-TYPE\t"+header.getMimetype()) );
        String date = header.getDate();
        if( date != null && date.length() > 4 ) {
            output.collect( new Text("content-types"), new Text("YEAR\t"+date.substring(0,4)) );
        } else {
            output.collect( new Text("malformed-date"), new Text("MALFORMED-DATE") );            
        }

        // TODO: Consider calling Normalisation.sanitiseWARCHeaderValue on the header.getUrl to guard against '<>'-encapsulation
        // URL:
        String uri = header.getUrl();
        if( uri != null ){ 
            UsableURI uuri = UsableURIFactory.getInstance(uri);
            // Hosts:
            if( "https".contains(uuri.getScheme()) ) {
                output.collect( new Text("record-hosts"), new Text("HOSTS\t"+uuri.getAuthority()) );
            }
        } else {
            output.collect( new Text("record-hosts"), new Text("NULL-URI-TOTAL") );            
        }

    }

}
