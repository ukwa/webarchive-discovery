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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wa.hadoop.ArchiveFileInputFormat;

@SuppressWarnings( "deprecation" )
public class OutlinkExtractor extends Configured implements Tool {

    private boolean wait = false;
    
    protected void createJobConf(JobConf conf, String[] args)
            throws IOException {
        String line = null;
        BufferedReader br = new BufferedReader( new FileReader( args[ 0 ] ) );
        while( ( line = br.readLine() ) != null ) {
            FileInputFormat.addInputPath( conf, new Path( line ) );
        }
        FileOutputFormat.setOutputPath( conf, new Path( args[ 1 ] ) );

        conf.setJobName( args[ 0 ] + "_" + System.currentTimeMillis() );
        conf.setInputFormat( ArchiveFileInputFormat.class );
        conf.setMapperClass( OutlinkExtractorMapper.class );
        conf.setReducerClass( FrequencyCountingReducer.class );
        conf.setOutputFormat( TextOutputFormat.class );

        conf.setOutputKeyClass( Text.class );
        conf.setOutputValueClass( Text.class );
    }

    public int run(String[] args) throws IOException {
        // Set up the base conf:
        JobConf conf = new JobConf(getConf(), OutlinkExtractor.class);

        // Get the job configuration:
        this.createJobConf(conf, args);

        // Submit it:
        if (this.wait) {
            JobClient.runJob(conf);
        } else {
            JobClient client = new JobClient(conf);
            client.submitJob(conf);
        }
        return 0;
    }

    public static void main( String[] args ) throws Exception {
        if( args.length != 2 ) {
            System.out.println( "Need input file list and output dir!" );
            System.exit( 1 );

        }
        int ret = ToolRunner.run( new OutlinkExtractor(), args );
        System.exit( ret );
    }
}
