package uk.bl.wap.hadoop;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.RESPONSE;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wap.util.warc.WARCRecordUtils;

@SuppressWarnings( "deprecation" )
public class ArchiveFileRecordReader<Key extends WritableComparable<?>, Value extends Writable> implements RecordReader<Text, WritableArchiveRecord> {
	private static Logger log = Logger.getLogger(ArchiveFileRecordReader.class.getName());
	
	private FSDataInputStream datainputstream;
	private FileStatus status;
	private FileSystem filesystem;
	private long maxPayloadSize;
	private String[] url_excludes;
	private String[] response_includes;
	private String[] protocol_includes;
	private Path[] paths;
	int currentPath = -1;
	Long offset = 0L;
	private ArchiveReader arcreader;
	private Iterator<ArchiveRecord> iterator;
	private ArchiveRecord record;
	private ArchiveRecordHeader header;
	private String archiveName;

	public ArchiveFileRecordReader( Configuration conf, InputSplit split ) throws IOException {
		this.maxPayloadSize = conf.getLong( "archive.size.max", 104857600L );
		log.warn("maxPayloadSize="+this.maxPayloadSize);
		
		this.url_excludes = conf.getStrings( "record.exclude.url", new String[] {} );
		log.warn("url_excludes="+this.printList(url_excludes));
		
		this.response_includes = conf.getStrings( "record.include.response", new String[] { "200" } );
		log.warn("response_includes="+this.printList(response_includes));
		
		this.protocol_includes = conf.getStrings( "record.include.protocol", new String[] { "http", "https" } ); 
		log.warn("protocol_includes="+this.printList(protocol_includes));
		
		this.filesystem = FileSystem.get( conf );

		if( split instanceof FileSplit ) {
			this.paths = new Path[ 1 ];
			this.paths[ 0 ] = ( ( FileSplit ) split ).getPath();
		} else if( split instanceof MultiFileSplit ) {
			this.paths = ( ( MultiFileSplit ) split ).getPaths();
		} else {
			throw new IOException( "InputSplit is not a file split or a multi-file split - aborting" );
		}
		// Log the paths:
		for( Path p : this.paths ) {
			log.warn("Processing path: "+p);
		}
		// Queue up the iterator:
		this.nextFile();
	}
	
	private String printList(String[] set ) {
		String list = "";
		for( String item : set ) {
			if( ! "".equals(list) ) list += ",";
			list += item;
		}
		return list;
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
			} catch( Throwable e ) {
				log.error( "ERROR in hasNext():  "+this.archiveName+": "+ e.toString() );
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
					} else {
						log.warn("Skipped record, length="+header.getLength()+" checkUrl="+checkUrl(url)+" checkProtocol="+checkProtocol(url));
					}
				} else if( !this.nextFile() ) {
					break;
				}
			} catch( Throwable e ) {
				found = false;
				log.error( "ERROR reading "+this.archiveName+": "+ e.toString() );
				//e.printStackTrace();
			}
		}
		return found;
	}

	private boolean nextFile() throws IOException {
		currentPath++;
		if( currentPath >= paths.length ) {
			return false;
		}
		// Output the archive filename, to help with debugging:
		log.info("Opening nextFile: " + paths[currentPath]);
		// Set up the ArchiveReader:
		this.status = this.filesystem.getFileStatus( paths[ currentPath ] );
		datainputstream = this.filesystem.open( paths[ currentPath ] );
		arcreader = ( ArchiveReader ) ArchiveReaderFactory.get( paths[ currentPath ].getName(), datainputstream, true );
		// Set to strict reading, in order to cope with malformed archive files which cause an infinite loop otherwise.
		arcreader.setStrict(true);
		// Get the iterator:
		iterator = arcreader.iterator();
		this.archiveName = paths[ currentPath ].getName();
		return true;
	}

	private boolean checkUrl( String url ) {
		for( String exclude : url_excludes ) {
			if( !"".equals(exclude) && url.matches( ".*" + exclude + ".*" ) ) {
				return false;
			}
		}
		return true;
	}

	private boolean checkProtocol( String url ) {
		for( String include : protocol_includes ) {
			if( "".equals(include) || url.startsWith( include ) ) {
				return true;
			}
		}
		return false;
	}

	private boolean checkResponse( String response ) {
		for( String include : response_includes ) {
			if( "".equals(include) || response.matches( include ) ) {
				return true;
			}
		}
		return false;
	}

}
