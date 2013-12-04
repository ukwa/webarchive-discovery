package uk.bl.wa.hadoop.mapreduce.lib;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

public class DereferencingArchiveToCDXRecordReader<Key extends WritableComparable<?>, Value extends Writable> extends RecordReader<Text, Text> {
	private static final Logger LOGGER = Logger.getLogger( DereferencingArchiveToCDXRecordReader.class.getName() );
	private static final String DNS =  "dns";
	private static final String CDX_SEPARATOR = " ";
	private static final String CDX_NULL_VALUE = "-";
	private static final String WARC_REVISIT = "revisit";
	private static final String WARC_RESPONSE = "response";
	private static final String WARC_REVISIT_MIME = "warc/revisit";
	private static final String HTTP_LOCATION = "Location";

	private AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();
	private LineRecordReader internal = new LineRecordReader();
	private FSDataInputStream datainputstream;
	private FileSystem filesystem;
	private WarcReader warcreader;
	private Iterator<WarcRecord> iwarc;
	private Text key;
	private Text value;
	private boolean hdfs;
	private HashMap<String, String> warcArkLookup = new HashMap<String, String>();

	@Override
	public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSplit fileSplit = ( FileSplit ) split;
		this.filesystem = fileSplit.getPath().getFileSystem( conf );
		this.hdfs = Boolean.parseBoolean( conf.get( "cdx.hdfs", "false" ) );
		internal.initialize( split, context );
		this.getLookup( conf );
	}

	private void getLookup( Configuration conf ) {
		try {
			URI[] uris = DistributedCache.getCacheFiles( conf );
			if( uris != null ) {
				for( URI uri : uris ) {
					FSDataInputStream input = this.filesystem.open( new Path( uri.getPath() ) );
					BufferedReader reader = new BufferedReader( new InputStreamReader( input ) );
					String line;
					String[] values;
					while( ( line = reader.readLine() ) != null ) {
						values = line.split( "\\s+" );
						warcArkLookup.put( values[ 0 ], values[ 1 ] );
					}
					System.out.println( "Added " + warcArkLookup.size() + " entries to ARK lookup." );
				}
			}
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		if( datainputstream != null ) {
			try {
				datainputstream.close();
			} catch( IOException e ) {
				System.err.println( "close(): " + e.getMessage() );
			}
		}
		try {
			internal.close();
		} catch( IOException e ) {
			LOGGER.error( "close(): " + e.getMessage() );
		}
	}

	@Override
	public float getProgress() throws IOException {
		return internal.getProgress();
	}

	@Override
	public boolean nextKeyValue() {
		if( this.key == null ) {
			this.key = new Text();
		}
		if( this.value == null ) {
			this.value = new Text();
		}
		String line;
		while( true ) {
			try {
				if( iwarc != null && iwarc.hasNext() ) {
					line = warcRecordToCDXLine( iwarc.next() );
					if( line != null ) {
						this.key.set( line );
						this.value.set( line );
						return true;
					}
				} else {
					if( this.internal.nextKeyValue() ) {
						Path path = new Path( this.internal.getCurrentValue().toString() );
						datainputstream = this.filesystem.open( path );
						warcreader = WarcReaderFactory.getReader( datainputstream );
						iwarc = warcreader.iterator();
					} else {
						return false;
					}
				}
			} catch( Exception e ) {
				LOGGER.error( "nextKeyValue: " + e.getMessage() );
			}
		}
	}

	private String warcRecordToCDXLine( WarcRecord record ) {
		WarcHeader header = record.header;
		// We're only processing response/revisit records and only for HTTP responses.
		if( !( header.warcTypeStr.equals( WARC_RESPONSE ) || header.warcTypeStr.equals( WARC_REVISIT ) ) || header.warcTargetUriStr.startsWith( DNS ) )
			return null;

		StringBuilder sb = new StringBuilder();
		// Canonicalized URL
		sb.append( canon.canonicalize( header.warcTargetUriStr ) );
		sb.append( CDX_SEPARATOR );
		// 14-digit Timestamp
		sb.append( header.warcDateStr.replaceAll( "[^0-9]", "" ) );
		sb.append( CDX_SEPARATOR );
		// URL
		sb.append( header.warcTargetUriStr );
		sb.append( CDX_SEPARATOR );
		// MIME, HTTP Status Code
		if( header.contentTypeStr.equals( WARC_REVISIT ) ) {
			sb.append( WARC_REVISIT_MIME );
			sb.append( CDX_SEPARATOR );
			sb.append( CDX_NULL_VALUE );
		} else {
			// Preferably use the WARC-Identified-Payload-Type header.
			if( header.warcIdentifiedPayloadTypeStr != null && !header.warcIdentifiedPayloadTypeStr.equals( "" ) ) {
				sb.append( header.warcIdentifiedPayloadTypeStr );
			} else {
				record.getHttpHeader().getProtocolContentType();
			}
			sb.append( CDX_SEPARATOR );
			sb.append( record.getHttpHeader().getProtocolStatusCodeStr() );
		}
		sb.append( CDX_SEPARATOR );
		// Hash
		sb.append( header.warcBlockDigestStr );
		sb.append( CDX_SEPARATOR );
		// '-' or Redirect URL
		if( !header.warcTypeStr.equals( WARC_REVISIT ) && record.getHttpHeader().getProtocolStatusCodeStr().startsWith( "3" ) ) {
			// HTTP headers *should* contain a Location line.
			if( record.getHttpHeader().getHeader( HTTP_LOCATION ) != null ) {
				sb.append( record.getHttpHeader().getHeader( HTTP_LOCATION ).value );
			} else {
				sb.append( CDX_NULL_VALUE );
			}
		} else {
			sb.append( CDX_NULL_VALUE );
		}
		sb.append( CDX_SEPARATOR );
		// -
		sb.append( CDX_NULL_VALUE );
		sb.append( CDX_SEPARATOR );
		// Offset
		sb.append( Long.toString( header.getStartOffset() ) );
		sb.append( CDX_SEPARATOR );
		// Identifier (filename, path or ARK) skipped.
		sb.append( getIdentifier() );
		return sb.toString();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	private String getIdentifier() {
		String fullPath = this.internal.getCurrentValue().toString();
		if( this.hdfs ) {
			if( warcArkLookup.size() != 0 ) {
				new File( fullPath ).getName();
				return warcArkLookup.get( new File( fullPath ).getName() );
			} else {
				return fullPath;
			}
		} else {
			return new File( fullPath ).getName();
		}
	}
}