package uk.bl.wa.hadoop.outlinks;

/*
 * #%L
 * warc-hadoop-indexer
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
import static org.archive.format.warc.WARCConstants.WARCRecordType;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.util.Normalisation;

@SuppressWarnings( "deprecation" )
public class OutlinkExtractorMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, Text> {
    Pattern pattern = Pattern.compile( "^(https?://([^/:]+)(:[0-9]+)?/).*$" );
    Matcher matcher = null;
    String resourceHost;
    String year;
    Text outputKey;
    String resourceUrl;
    Iterator<String> links;
    ArchiveRecordHeader header;

    @Override
    public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
        try {
            header = value.getRecord().getHeader();
            // If this is a non-response WARC record...
            if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) && 
               !header.getHeaderValue( HEADER_KEY_TYPE ).equals( WARCRecordType.response.toString() ) ) {
                return;
            }
            resourceUrl = Normalisation.sanitiseWARCHeaderValue(value.getRecord().getHeader().getUrl());
            // ..or if this isn't a HTTP record...
            if( !resourceUrl.startsWith( "http" ) ) {
                return;
            }
            matcher = pattern.matcher( resourceUrl );
            if( matcher.matches() ) {
                resourceHost = matcher.group( 2 );
                year = value.getRecord().getHeader().getDate().substring( 0, 4 );
                outputKey = new Text( year + "\t" + resourceHost );

                Metadata metadata = HtmlFeatureParser.extractMetadata( value.getRecord(), resourceUrl );
                for( String link : metadata.getValues( HtmlFeatureParser.LINK_LIST ) ) {
                    matcher = pattern.matcher( link );
                    if( matcher.matches() ) {
                        output.collect( outputKey, new Text( matcher.group( 2 ) ) );
                    }
                }
            }
        } catch( Exception e ) {
            System.err.println( e.getMessage() );
        }
    }
}
