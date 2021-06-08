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

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings( "deprecation" )
public class HostsReport extends Configured implements Tool {
    private static Logger log = LoggerFactory.getLogger(HostsReport.class.getName() );

    @Override
    public int run( String[] args ) throws Exception {
        JobConf conf = new JobConf( getConf(), HostsReport.class );

        log.info( "Adding logs..." );
        String line;
        BufferedReader br = new BufferedReader( new FileReader( args[ 0 ] ) );
        while( ( line = br.readLine() ) != null ) {
            log.info( "Adding " + line );
            FileInputFormat.addInputPath( conf, new Path( line ) );
        }
        br.close();

        FileOutputFormat.setOutputPath( conf, new Path( args[ 1 ] ) );
        conf.setJarByClass( HostsReport.class );
        conf.setInputFormat( TextInputFormat.class );
        conf.setMapperClass( HostsReportMapper.class );
        conf.setMapOutputKeyClass( Text.class );
        conf.setMapOutputValueClass( Text.class );
        conf.setCombinerClass( HostsReportReducer.class );
        conf.setReducerClass( HostsReportReducer.class );
        conf.setOutputFormat( TextOutputFormat.class );

        JobClient.runJob( conf );
        return 0;
    }

    public static void main( String[] args ) throws Exception {
        if( args.length != 2 ) {
            System.out.println( "Need <input file list> and <output dir>!" );
            System.exit( 1 );

        }
        int ret = ToolRunner.run( new HostsReport(), args );
        System.exit( ret );
    }
}
