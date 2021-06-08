package uk.bl.wa.hadoop.hosts;

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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@SuppressWarnings( "deprecation" )
public class HostsReportMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    Pattern pattern = Pattern.compile( "^https?://([^/]+)/.*$" );
    Matcher matcher;

    @Override
    public void map( LongWritable offset, Text log, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
        // crawl.log
        // LogTimestamp StatusCode Size URI DiscoveryPath Referrer MIME ThreadID RequestTimestamp+Duration Digest - Annotations
        String[] entries = log.toString().split( "\\s+" );
        if( entries.length != 12 )
            return;
        // Skip invalid responses
        if( entries[ 1 ].startsWith( "-" ) && !entries[ 1 ].equals( "-9998" ) ) {
            return;
        }
        // hosts-report.txt
        // #urls #bytes ^host #robots #^remaining #^novel-urls #^novel-bytes #dup-by-hash-urls #dup-by-hash-bytes #^not-modified-urls #^not-modified-bytes
        StringBuilder sb = new StringBuilder();
        // #urls
        sb.append( "1 " );
        // #bytes
        if( entries[ 2 ].equals( "-" ) )
            sb.append( "0 " );
        else
            sb.append( entries[ 2 ] + " " );
        // #robots
        if( entries[ 1 ].equals( "-9998" ) )
            sb.append( "1 " );
        else
            sb.append( "0 " );
        // #dup-by-hash, #dup-by-hash-bytes
        if( entries[ 11 ].contains( "warcRevists:digest" ) ) {
            sb.append( "1 " );
            if( entries[ 2 ].equals( "-" ) )
                sb.append( "0 " );
            else
                sb.append( entries[ 2 ] + " " );
        } else {
            sb.append( "0 " );
            sb.append( "0 " );
        }
        String host;
        if( entries[ 3 ].startsWith( "dns:" ) ) {
            host = entries[ 3 ].replace( "dns:", "" );
        } else {
            matcher = pattern.matcher( entries[ 3 ] );
            if( matcher.matches() ) {
                host = matcher.group( 1 );
            } else {
                return;
            }
        }
        output.collect( new Text( host ), new Text( sb.toString() ) );
    }
}
