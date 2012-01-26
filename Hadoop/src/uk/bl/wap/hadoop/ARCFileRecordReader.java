package uk.bl.wap.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;

@SuppressWarnings( "deprecation" )
public class ARCFileRecordReader<Key extends WritableComparable<?>, Value extends Writable> implements RecordReader<Text, WritableARCRecord> {
	private static final String CONFIG = "/hadoop_utils.config";
	private FSDataInputStream datainputstream = null;
	private FileStatus status = null;
	private FileSystem filesystem = null;
	private long maxPayloadSize = 104857600L;
	private String[] url_excludes;
	private String[] response_includes;
	private String[] protocol_includes;
	private Path[] paths = null;
	int currentPath = -1;
	Long offset = 0L;
	private ARCReader arcreader = null;
	private Iterator<ArchiveRecord> arciterator = null;
	private ARCRecord arcrecord = null;
	private String arcName = null;

	public ARCFileRecordReader( Configuration conf, InputSplit split ) throws IOException {
		Properties properties = new Properties();
		try {
			properties.load( this.getClass().getResourceAsStream( ( CONFIG ) ) );
			this.maxPayloadSize = Long.parseLong( properties.getProperty( "max_payload_size" ) );
			this.url_excludes = properties.getProperty( "url_exclude" ).replaceAll( "\\.", "\\\\." ).split( "," );
			this.response_includes = properties.getProperty( "response_include" ).split( "," );
			this.protocol_includes = properties.getProperty( "protocol_include" ).split( "," );
		} catch( IOException i ) {
			System.err.println( "Could not find Properties file: " + i.getMessage() );
		}
		this.filesystem = FileSystem.get( conf );

		if( split instanceof FileSplit ) {
			this.paths = new Path[ 1 ];
			this.paths[ 0 ] = ( ( FileSplit ) split ).getPath();
		} else if( split instanceof MultiFileSplit ) {
			this.paths = ( ( MultiFileSplit ) split ).getPaths();
		} else {
			throw new IOException( "InputSplit is not a file split or a multi-file split - aborting" );
		}
		this.nextFile();
	}

	@Override
	public void close() throws IOException {
		if( datainputstream != null ) {
			try {
				datainputstream.close();
			} catch( IOException e ) {
				System.err.println( "close(): " + e.getMessage() );
			}
		}
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public WritableARCRecord createValue() {
		return new WritableARCRecord();
	}

	@Override
	public long getPos() throws IOException {
		return datainputstream.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		return datainputstream.getPos() / ( 1024 * 1024 * this.status.getLen() );
	}

	@Override
	public boolean next( Text key, WritableARCRecord value ) throws IOException {
		boolean found = false;
		while( !found ) {
			boolean hasNext = false;
			try {
				 hasNext = arciterator.hasNext();
			} catch( Exception e ) {
				System.err.println( e.toString() );
				hasNext = false;
			}
			try {
				if( hasNext ) {
					arcrecord = ( ARCRecord ) arciterator.next();
					String url = ( String ) arcrecord.getHeader().getUrl();
	
	//					value.setHttpHeaders( WARCRecordUtils.getHeaders( arcrecord, true ) );
					if( arcrecord.getHeader().getLength() <= maxPayloadSize &&
						this.checkUrl( url ) &&
						this.checkProtocol( url ) ) {
	//						value.setHttpHeaders( WARCRecordUtils.getHeaders( arcrecord, true ) );
	//						if( value.getHttpHeader( "bl_status" ) != null &&
	//								this.checkResponse( value.getHttpHeader( "bl_status" ).split( " " )[ 1 ] ) ) {
								found = true;
								key.set( this.arcName );
								value.setRecord( arcrecord );
	//						}
					}
				} else if( !this.nextFile() ) {
					break;
				}
			} catch( Exception e ) {
				found = false;
				System.err.println( e.toString() );
			}
		}
		return found;
	}

	private boolean nextFile() throws IOException {
		currentPath++;
		if( currentPath >= paths.length ) {
			return false;
		}
		this.status = this.filesystem.getFileStatus( paths[ currentPath ] );
		datainputstream = this.filesystem.open( paths[ currentPath ] );
		arcreader = ( ARCReader ) ARCReaderFactory.get( "", datainputstream, true );
		arciterator = arcreader.iterator();
		this.arcName = paths[ currentPath ].getName();
		return true;
	}

	private boolean checkUrl( String url ) {
		for( String exclude : url_excludes ) {
			if( url.matches( ".*" + exclude + ".*" ) ) {
				return false;
			}
		}
		return true;
	}

	private boolean checkProtocol( String url ) {
		for( String include : protocol_includes ) {
			if( url.startsWith( include ) ) {
				return true;
			}
		}
		return false;
	}

	private boolean checkResponse( String response ) {
		for( String include : response_includes ) {
			if( response.matches( include ) ) {
				return true;
			}
		}
		return false;
	}

}
