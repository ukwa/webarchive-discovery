package uk.bl.wap.hadoop;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.RESPONSE;

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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wap.util.warc.WARCRecordUtils;

@SuppressWarnings( "deprecation" )
public class ArchiveFileRecordReader<Key extends WritableComparable<?>, Value extends Writable> implements RecordReader<Text, WritableArchiveRecord> {
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
	private ArchiveReader arcreader = null;
	private Iterator<ArchiveRecord> iterator = null;
	private ArchiveRecord record = null;
	private ArchiveRecordHeader header = null;
	private String archiveName = null;

	public ArchiveFileRecordReader( Configuration conf, InputSplit split ) throws IOException {
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
	public WritableArchiveRecord createValue() {
		return new WritableArchiveRecord();
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
	public boolean next( Text key, WritableArchiveRecord value ) throws IOException {
		boolean found = false;
		while( !found ) {
			boolean hasNext = false;
			try {
				 hasNext = iterator.hasNext();
			} catch( Exception e ) {
				System.err.println( e.toString() );
				hasNext = false;
			}
			try {
				if( hasNext ) {
					record = ( ArchiveRecord ) iterator.next();
					header = record.getHeader();
					String url = header.getUrl();

					if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) && !header.getHeaderValue( HEADER_KEY_TYPE ).equals( RESPONSE ) ) {
						continue;
					}
					if( header.getLength() <= maxPayloadSize &&
						this.checkUrl( url ) &&
						this.checkProtocol( url ) ) {
							String http = WARCRecordUtils.getHeaders( record, true );
							value.setHttpHeaders( http );
							if( value.getHttpHeader( "bl_status" ) != null &&
									this.checkResponse( value.getHttpHeader( "bl_status" ).split( " " )[ 1 ] ) ) {
								found = true;
								key.set( this.archiveName );
								value.setRecord( record );
							}
					}
				} else if( !this.nextFile() ) {
					break;
				}
			} catch( Exception e ) {
				found = false;
				e.printStackTrace();
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
		arcreader = ( ArchiveReader ) ArchiveReaderFactory.get( paths[ currentPath ].getName(), datainputstream, true );
		iterator = arcreader.iterator();
		this.archiveName = paths[ currentPath ].getName();
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
