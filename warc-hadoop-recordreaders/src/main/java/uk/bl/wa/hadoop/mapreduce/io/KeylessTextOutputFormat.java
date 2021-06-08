package uk.bl.wa.hadoop.mapreduce.io;

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

/**
 * @author rcoram
 *         A copy of org.apache.hadoop.mapreduce.lib.output.TextOutputFormat, save for writing out the key/separator.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/** An {@link OutputFormat} that writes plain text files. */
public class KeylessTextOutputFormat<K, V> extends FileOutputFormat<K, V> {
    protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        static {
            try {
                newline = "\n".getBytes( utf8 );
            } catch( UnsupportedEncodingException uee ) {
                throw new IllegalArgumentException( "can't find " + utf8 + " encoding" );
            }
        }

        protected DataOutputStream out;

        public LineRecordWriter( DataOutputStream out, String keyValueSeparator ) {
            this.out = out;
        }

        public LineRecordWriter( DataOutputStream out ) {
            this( out, "\t" );
        }

        /**
         * Write the object to the byte stream, handling Text as a special
         * case.
         * 
         * @param o
         *            the object to print
         * @throws IOException
         *             if the write throws, we pass it on
         */
        private void writeObject( Object o ) throws IOException {
            if( o instanceof Text ) {
                Text to = ( Text ) o;
                out.write( to.getBytes(), 0, to.getLength() );
            } else {
                out.write( o.toString().getBytes( utf8 ) );
            }
        }

        public synchronized void write( K key, V value ) throws IOException {

            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if( nullKey && nullValue ) {
                return;
            }
            if( !nullValue ) {
                writeObject( value );
            }
            out.write( newline );
        }

        public synchronized void close( TaskAttemptContext context ) throws IOException {
            out.close();
        }
    }

    public RecordWriter<K, V> getRecordWriter( TaskAttemptContext job ) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput( job );
        String keyValueSeparator = conf.get( "mapred.textoutputformat.separator", "\t" );
        CompressionCodec codec = null;
        String extension = "";
        if( isCompressed ) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass( job, GzipCodec.class );
            codec = ( CompressionCodec ) ReflectionUtils.newInstance( codecClass, conf );
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile( job, extension );
        FileSystem fs = file.getFileSystem( conf );
        if( !isCompressed ) {
            FSDataOutputStream fileOut = fs.create( file, false );
            return new LineRecordWriter<K, V>( fileOut, keyValueSeparator );
        } else {
            FSDataOutputStream fileOut = fs.create( file, false );
            return new LineRecordWriter<K, V>( new DataOutputStream( codec.createOutputStream( fileOut ) ), keyValueSeparator );
        }
    }
}
