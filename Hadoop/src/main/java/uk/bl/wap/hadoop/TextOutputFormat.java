package uk.bl.wap.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;

/**
 * @author rcoram
 * A copy of org.apache.hadoop.mapred.TextOutputFormat, minus the Key/separator.
 */

@SuppressWarnings( "deprecation" )
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes( utf8 );
			} catch( UnsupportedEncodingException uee ) {
				throw new IllegalArgumentException( "can't find " + utf8 + " encoding" );
			}
		}
		long fileSizeLimit = 1024 * 1024 * 1024 * 10; //10MB

		protected DataOutputStream out;

		public LineRecordWriter( DataOutputStream out ) {
			this.out = out;
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

		@Override
		public void close( Reporter reporter ) throws IOException {
			out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter( FileSystem ignored, JobConf job, String name, Progressable progress ) throws IOException {
		boolean isCompressed = getCompressOutput( job );
		if( !isCompressed ) {
			Path file = FileOutputFormat.getTaskOutputPath( job, name );
			FileSystem fs = file.getFileSystem( job );
			FSDataOutputStream fileOut = fs.create( file, progress );
			return new LineRecordWriter<K, V>( fileOut );
		} else {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass( job, GzipCodec.class );
			CompressionCodec codec = ReflectionUtils.newInstance( codecClass, job );
			Path file = FileOutputFormat.getTaskOutputPath( job, name + codec.getDefaultExtension() );
			FileSystem fs = file.getFileSystem( job );
			FSDataOutputStream fileOut = fs.create( file, progress );
			return new LineRecordWriter<K, V>( new DataOutputStream( codec.createOutputStream( fileOut ) ) );
		}
	}
}
