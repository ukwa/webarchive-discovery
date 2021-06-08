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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import uk.bl.wa.hadoop.mapreduce.MutableInt;

@SuppressWarnings( { "deprecation" } )
/**
 * 
 */
public class FrequencyCountingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce( Text key, Iterator<Text> iterator, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException {

        // Use a simple set to collect only distinct results for this key:
        Map<String, MutableInt> matches = new HashMap<String, MutableInt>();
        while( iterator.hasNext() ) {
            String m = iterator.next().toString();
            // Get or set up the counter:
            MutableInt value = matches.get( m );
            if( value == null ) {
                value = new MutableInt();
                matches.put( m, value );
            }
            // Increment the counter for this match:
            value.inc();
        }

        // Loop through and collect all distinct matches:
        for( String match : matches.keySet() ) {
            // This ignores the count:
            // output.collect( key, new Text( match ) );
            // This also outputs the count:
            output.collect( key, new Text( match + "\t" + matches.get( match ).get() ) );
        }

    }
}
