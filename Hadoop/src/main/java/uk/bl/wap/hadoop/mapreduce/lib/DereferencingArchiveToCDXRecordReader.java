package uk.bl.wap.hadoop.mapreduce.lib;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.arc.ARCReader;
import org.archive.io.warc.WARCReader;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXLineAdapter;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;

public class DereferencingArchiveToCDXRecordReader<Key extends WritableComparable<?>, Value extends Writable> extends RecordReader<Text, Text> {
	LineRecordReader internal = new LineRecordReader();
	private FSDataInputStream datainputstream;
	private FileSystem filesystem;
	private ArchiveReader arcreader;
	private Iterator<CaptureSearchResult> archiveIterator;
	private Iterator<String> cdxlines;
	private WarcIndexer warcIndexer = new WarcIndexer();
	private ArcIndexer arcIndexer = new ArcIndexer();
	private Text key;
	private Text value;

	@Override
	public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSplit fileSplit = ( FileSplit ) split;
		this.filesystem = fileSplit.getPath().getFileSystem( conf );
		internal.initialize( split, context );
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
			System.err.println( "close(): " + e.getMessage() );
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
					line = cdxlines.next();
					this.key.set( line );
					this.value.set( line );
					return true;
				} else {
					if( this.internal.nextKeyValue() ) {
						Path path = new Path( this.internal.getCurrentValue().toString() );
						datainputstream = this.filesystem.open( path );
						arcreader = ArchiveReaderFactory.get( path.getName(), datainputstream, true );
						if( path.getName().matches( "^.+\\.warc(\\.gz)?$" ) ) {
							archiveIterator = warcIndexer.iterator( ( WARCReader ) arcreader );
						} else {
							archiveIterator = arcIndexer.iterator( ( ARCReader ) arcreader );
						}
						cdxlines = SearchResultToCDXLineAdapter.adapt( archiveIterator );
					} else {
						return false;
					}
				}
			} catch( Exception e ) {
				System.err.println( "nextKeyValue: " + e.getMessage() );
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
}
