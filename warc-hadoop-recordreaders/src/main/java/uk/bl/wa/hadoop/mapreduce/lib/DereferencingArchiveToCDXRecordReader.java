package uk.bl.wa.hadoop.mapreduce.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.arc.ARCReader;
import org.archive.io.warc.WARCReader;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXFormatAdapter;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;

public class DereferencingArchiveToCDXRecordReader<Key extends WritableComparable<?>, Value extends Writable> extends RecordReader<Text, Text> {
	private static final Logger LOGGER = Logger.getLogger( DereferencingArchiveToCDXRecordReader.class.getName() );
	private LineRecordReader internal = new LineRecordReader();
	private FSDataInputStream datainputstream;
	private FileSystem filesystem;
	private ArchiveReader arcreader;
	private Iterator<CaptureSearchResult> archiveIterator;
	private Iterator<String> cdxlines;
	private WarcIndexer warcIndexer = new WarcIndexer();
	private ArcIndexer arcIndexer = new ArcIndexer();
	private Text key;
	private Text value;
	private CDXFormat cdxFormat;
	private boolean hdfs;
	private HashMap<String, String> warcArkLookup = new HashMap<String, String>();

	@Override
	public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSplit fileSplit = ( FileSplit ) split;
		this.filesystem = fileSplit.getPath().getFileSystem( conf );
		try {
			this.cdxFormat = new CDXFormat( conf.get( "cdx.format", " CDX A b a m s k r M V g" ) );
			this.hdfs = Boolean.parseBoolean( conf.get( "cdx.hdfs", "false" ) );
		} catch( CDXFormatException e ) {
			LOGGER.error( "initialize(): " + e.getMessage() );
		}
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
				if( cdxlines != null && cdxlines.hasNext() ) {
					if( this.hdfs ) {
						line = hdfsPath( cdxlines.next(), this.internal.getCurrentValue().toString() );
					} else {
						line = cdxlines.next();
					}
					this.key.set( line );
					this.value.set( line );
					return true;
				} else {
					if( this.internal.nextKeyValue() ) {
						Path path = new Path( this.internal.getCurrentValue().toString() );
						datainputstream = this.filesystem.open( path );
						arcreader = ArchiveReaderFactory.get( path.getName(), datainputstream, true );
						arcreader.setStrict( false );
						if( path.getName().matches( "^.+\\.warc(\\.gz)?$" ) ) {
							archiveIterator = warcIndexer.iterator( ( WARCReader ) arcreader );
						} else {
							archiveIterator = arcIndexer.iterator( ( ARCReader ) arcreader );
						}
						cdxlines = SearchResultToCDXFormatAdapter.adapt( archiveIterator, cdxFormat );
					} else {
						return false;
					}
				}
			} catch( Exception e ) {
				LOGGER.error( "nextKeyValue: " + e.getMessage() );
			}
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	private String hdfsPath( String cdx, String path ) throws URISyntaxException {
		String[] fields = cdx.split( " " );
		if( warcArkLookup.size() != 0 ) {
			fields[ 9 ] = warcArkLookup.get( fields[ 9 ] );
		} else {
			fields[ 9 ] = new URI( path ).getPath() + "?user.name=hadoop&bogus=.warc.gz";
		}
		return StringUtils.join( fields, " " );
	}
}