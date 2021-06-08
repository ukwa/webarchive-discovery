package uk.bl.wa.hadoop.entities;

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

import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.URIException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.Tika;
import org.archive.io.ArchiveRecordHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.util.Normalisation;

@SuppressWarnings( { "deprecation" } )
public class EntityMapper extends MapReduceBase implements Mapper<Text, WritableArchiveRecord, Text, Text> {
    
    private static Logger log = LoggerFactory.getLogger(EntityMapper.class.getName());
    
    private Pattern pattern;
    
    Tika tika = new Tika();
    
    public EntityMapper() {}

    @Override
    public void configure( JobConf job ) {
        this.pattern = Pattern.compile( job.get( EntityExtractor.REGEX_PATTERN_PARAM ) ); 
    }
    
    @Override
    public void map( Text key, WritableArchiveRecord value, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {
        ArchiveRecordHeader header = value.getRecord().getHeader();

        /*
        // Determine the type:
        try {
            String tikaType = tika.detect(value.getPayload());
            // return without collecting anything if this is neither HTML or XHTML:
            if( ! tikaType.startsWith("application/xhtml+xml") &&
                    ! tikaType.startsWith("text/html") );
            return;
        } catch( Throwable e ) {
            log.error( "Tika.detect failed:" + e.getMessage() );
            //e.printStackTrace();
        }
        */
        
        // Generate the key:
        String newKey = "0000";//"0/unknown";            
        if( !header.getHeaderFields().isEmpty() ) {
            //newKey = header.getDate() + "/" + header.getUrl();
            // Reduce this to just the year and the host:
            String year = WARCIndexer.extractYear(header.getDate());
            //String host = extractHost(header.getUrl());
            //newKey = year + "/" + host;
            newKey = year;
        }
        
        // Collect the linkages
        String base_url = Normalisation.sanitiseWARCHeaderValue(value.getRecord().getHeader().getUrl());
        String sourceSuffix = LinkExtractor.extractPublicSuffix( base_url );
        if( sourceSuffix == null ) sourceSuffix = "null";
        Set<String> destSuffixes = null;
        try {
            destSuffixes= LinkExtractor.extractPublicSuffixes(HtmlFeatureParser.extractMetadata(value.getRecord(), base_url));
        } catch( java.nio.charset.UnsupportedCharsetException e ) {
            log.error("Could not parse record! "+e);
            return;
        } catch( java.nio.charset.IllegalCharsetNameException e ) {
            log.error("Could not parse record! "+e);
            return;
        } catch( Exception e) {
            log.error("Could not parse record! "+e);
            return;
        }
        
        // Pass out the mapped results as in-links by year:
        for( String destSuffix : destSuffixes ) {
            output.collect( new Text( newKey+"\t"+destSuffix ), new Text( sourceSuffix ) );            
        }
    }
    

}
