package uk.bl.wa.util.solr;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.Tika;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.restlet.data.MediaType;
import org.xml.sax.ContentHandler;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.extract.Times;


/**
 * c.f. uk.bl.wap.tika.TikaDeepIdentifier
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TikaExtractor {
	
	public static final String TIKA_PARSE_EXCEPTION = "Tika-Parse-Exception";

	private static Log log = LogFactory.getLog(TikaExtractor.class);
	
	/** Time to wait for Tika to complete before giving up: */
	private long parseTimeout;
	
	/**
	 *  MIME types to exclude from parsing:
	 *  e.g.
# Javascript/CSS excluded as irrelevant.
# Archives excluded as these seem to cause Java heap space errors with Tika.
mime_exclude = x-tar,x-gzip,bz,lz,compress,zip,javascript,css,octet-stream,image,video,audio
     *
     */
	private List<String> excludes;
	
	/** The actual Tika instance */
	private Tika tika;
	
	/** Maximum number of characters of text to pull out of any given resource: */
	private Long max_text_length; 

	/* --- --- --- --- */
	
	public TikaExtractor() {
		this( ConfigFactory.load() );
	}

	/**
	 * 
	 * @param conf
	 */
	public TikaExtractor( Config conf ) {
		this.tika = new Tika();
		
		this.excludes = conf.getStringList( "warc.index.tika.exclude_mime" );
		log.info("Config: MIME exclude list: " + this.excludes);
		
		this.parseTimeout = conf.getLong( "warc.index.tika.parse_timeout");
		log.info("Config: Parser timeout (ms) " + parseTimeout);
		
		this.max_text_length = conf.getBytes( "warc.index.tika.max_text_length");
		log.info("Config: Maximum length of text to extract (characters) "+ this.max_text_length);
	}


	
	/**
	 * Override embedded document parser logic to prevent descent.
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 *
	 */
	public class NonRecursiveEmbeddedDocumentExtractor extends ParsingEmbeddedDocumentExtractor {

		/** Parse embedded documents? Defaults to FALSE */
		private boolean parseEmbedded = false;

		public NonRecursiveEmbeddedDocumentExtractor(ParseContext context) {
			super(context);
		}

		/* (non-Javadoc)
		 * @see org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor#shouldParseEmbedded(org.apache.tika.metadata.Metadata)
		 */
		@Override
		public boolean shouldParseEmbedded(Metadata metadata) {
			return this.parseEmbedded;
		}
		
		/**
		 * @return the parseEmbedded
		 */
		public boolean isParseEmbedded() {
			return parseEmbedded;
		}

		/**
		 * @param parseEmbedded the parseEmbedded to set
		 */
		public void setParseEmbedded(boolean parseEmbedded) {
			this.parseEmbedded = parseEmbedded;
		}

	}
	
	private NonRecursiveEmbeddedDocumentExtractor embedded = null;
	
	/**
	 * 
	 * @param solr
	 * @param is
	 * @param url
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings( "deprecation" )
	public SolrRecord extract( SolrRecord solr, InputStream is, String url ) throws IOException {
		
		// Set up the TikaInputStream:
		TikaInputStream tikainput = TikaInputStream.get( new CloseShieldInputStream(is) );
		
		// Also pass URL as metadata to allow extension hints to work:
		Metadata metadata = new Metadata();
		if( url != null )
			metadata.set( Metadata.RESOURCE_NAME_KEY, url);

		StringBuilder detected = new StringBuilder();
		try {
			DetectRunner detect = new DetectRunner( tika, tikainput, detected );
			Thread detectThread = new Thread( detect, Long.toString( System.currentTimeMillis() ) );
			detectThread.start();
			detectThread.join( 10000L );
			detectThread.interrupt();
		} catch( NoSuchFieldError e ) {
			// TODO Is this an Apache POI version issue?
			log.error( "Tika.detect(): " + e.getMessage() );
			addExceptionMetadata(metadata, new Exception("detect threw "+e.getClass().getCanonicalName()) );
		} catch( Exception e ) {
			log.error( "Tika.detect(): " + e.getMessage() );
			addExceptionMetadata(metadata, e);
		}
		
		// Only proceed if we have a suitable type:
		if( !this.checkMime( detected.toString() ) ) {
			solr.addField( SolrFields.SOLR_CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM.toString() );
			return solr;
		}
		
		// Context
		ParseContext context = new ParseContext();
		StringWriter content = new StringWriter();
		
		// Override the recursive parsing:
		if( embedded == null )
			embedded = new NonRecursiveEmbeddedDocumentExtractor(context);
		context.set( EmbeddedDocumentExtractor.class, embedded );
		
		try {
			ParseRunner runner = new ParseRunner( tika.getParser(), tikainput, this.getHandler( content ), metadata, context );
			Thread parseThread = new Thread( runner, Long.toString( System.currentTimeMillis() ) );
			try {
				parseThread.start();
				parseThread.join( this.parseTimeout );
				parseThread.interrupt();
			} catch( OutOfMemoryError o ) {
				log.error( "TikaExtractor.parse() - OutOfMemoryError: " + o.getMessage() );
				addExceptionMetadata(metadata, new Exception("OutOfMemoryError"));
			} catch( RuntimeException r ) {
				log.error( "TikaExtractor.parse() - RuntimeException: " + r.getMessage() );
				addExceptionMetadata(metadata, r);
			}
			
			// If there was a parse error, report it:
			solr.addField( SolrFields.PARSE_ERROR, metadata.get( TikaExtractor.TIKA_PARSE_EXCEPTION ) );

			// Copy the body text:
			String output = content.toString();
			if( runner.complete || !output.equals( "" ) ) {
				//log.debug("Extracted text from: "+url);				
				solr.doc.setField( SolrFields.SOLR_EXTRACTED_TEXT, output );
				solr.doc.setField( SolrFields.SOLR_EXTRACTED_TEXT_LENGTH, Integer.toString( output.length() ) );
			} else {
				//log.debug("Failed to extract any text from: "+url);
			}
			
			/*
			// Noisily report all metadata properties:
			for( String m : metadata.names() ) {
				log.info("For "+url.substring(url.length() - (int) Math.pow(url.length(),0.85))+": "+m+" -> "+metadata.get(m));
			}
			*/
			
			String contentType = metadata.get( Metadata.CONTENT_TYPE );
			solr.addField( SolrFields.SOLR_CONTENT_TYPE, contentType );
			solr.addField( SolrFields.SOLR_TITLE, metadata.get( DublinCore.TITLE ) );
			solr.addField( SolrFields.SOLR_DESCRIPTION, metadata.get( DublinCore.DESCRIPTION ) );
			solr.addField( SolrFields.SOLR_AUTHOR, metadata.get( DublinCore.CREATOR) );
			solr.addField( SolrFields.CONTENT_ENCODING, metadata.get( Metadata.CONTENT_ENCODING ) );

			// Parse out any embedded date that can act as a created/modified date.
			String date = null;
			if( metadata.get( Metadata.CREATION_DATE ) != null)
				date = metadata.get( Metadata.CREATION_DATE );
			if( metadata.get( Metadata.DATE ) != null)
				date = metadata.get( Metadata.DATE );
			if( metadata.get( Metadata.MODIFIED ) != null)
				date = metadata.get( Metadata.MODIFIED );
			if( date != null ) {
				DateTimeFormatter df = ISODateTimeFormat.dateTimeParser();
				DateTime edate = null;
				try {
					edate = df.parseDateTime(date);
				} catch( IllegalArgumentException e ) {
					log.error( "Could not parse: "+ date );
				}
				if( edate == null ) {
					Date javadate = Times.extractDate(date);
					if( javadate != null )
						edate = new org.joda.time.DateTime( javadate );
				}
				if( edate != null ) {
					solr.addField( SolrFields.LAST_MODIFIED_YEAR, ""+edate.getYear() );
					DateTimeFormatter iso_df = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC);
					solr.addField( SolrFields.LAST_MODIFIED, iso_df.print( edate ) );
				}
			}
			
			// Also look to record the software:
			String software = null;
			// For PDF, create separate tags:
			if( contentType != null && contentType.startsWith("application/pdf") ) {
				
				// PDF has Creator and Producer application properties:
				String creator = metadata.get("pdf:creator");
				if( creator != null ) solr.addField(SolrFields.GENERATOR, creator+ " (pdf:creator)");
				String producer = metadata.get("pdf:producer");
				if( producer != null) solr.addField(SolrFields.GENERATOR, producer+" (pdf:producer)");
			}
			// Application ID, MS Office only AFAICT, and the VERSION is only doc
			if( metadata.get( Metadata.APPLICATION_NAME ) != null ) software = metadata.get( Metadata.APPLICATION_NAME );
			if( metadata.get( Metadata.APPLICATION_VERSION ) != null ) software += " "+metadata.get( Metadata.APPLICATION_VERSION);
			// Images, e.g. JPEG and TIFF, can have 'Software', 'tiff:Software',
			if( metadata.get( "Software" ) != null ) software = metadata.get( "Software" );
			if( metadata.get( Metadata.SOFTWARE ) != null ) software = metadata.get( Metadata.SOFTWARE );
			if( metadata.get( "generator" ) != null ) software = metadata.get( "generator" );
			// PNGs have a 'tEXt tEXtEntry: keyword=Software, value=GPL Ghostscript 8.71'
			String png_textentry = metadata.get("tEXt tEXtEntry");
			if( png_textentry != null && png_textentry.contains("keyword=Software, value=") )
				software = png_textentry.replace("keyword=Software, value=", "");
			/* Some JPEGs have this:
	Jpeg Comment: CREATOR: gd-jpeg v1.0 (using IJG JPEG v62), default quality
	comment: CREATOR: gd-jpeg v1.0 (using IJG JPEG v62), default quality
			 */
			if( software != null ) {
				solr.addField(SolrFields.GENERATOR, software);
			}
			
		} catch( Exception e ) {
			log.error( "TikaExtractor.extract(): " + e.getMessage() );
		}
		return solr;
	}

	private class ParseRunner implements Runnable {
		private Parser parser;
		private InputStream tikainput;
		private ContentHandler handler;
		private Metadata metadata;
		private ParseContext context;
		private boolean complete;

		public ParseRunner( Parser parser, InputStream tikainput, ContentHandler handler, Metadata metadata, ParseContext context ) {
			this.parser = parser;
			this.tikainput = tikainput;
			this.handler = handler;
			this.metadata = metadata;
			this.context = context;
			this.complete = false;
		}

		@Override
		public void run() {
			try {
				this.parser.parse( this.tikainput, this.handler, this.metadata, this.context );
				this.complete = true;
			} catch( InterruptedIOException i ) {
				this.complete = false;
				log.error( "ParseRunner.run() Interrupted: " + i.getMessage() );
				addExceptionMetadata(metadata, i);
			} catch( Exception e ) {
				log.error( "ParseRunner.run() Exception: " + ExceptionUtils.getRootCauseMessage(e));
				addExceptionMetadata(metadata, e);
			}
		}
		
	}

	private class DetectRunner implements Runnable {
		private Tika tika;
		private InputStream input;
		private StringBuilder mime;

		public DetectRunner( Tika tika, InputStream input, StringBuilder mime ) {
			this.tika = tika;
			this.input = input;
			this.mime = mime;
		}

		@Override
		public void run() {
			try {
				mime.append( this.tika.detect( this.input ) );
			} catch( NoSuchFieldError e ) {
				// Apache POI version issue?
				log.error( "Tika.detect(): " + e.getMessage() );
			} catch( Exception e ) {
				log.error( "Tika.detect(): " + e.getMessage() );
			}
		}
	}
	
	/**
	 * Support storing the error message for analysis:
	 * 
	 * @param e
	 */
	public static void addExceptionMetadata( Metadata metadata, Exception e ) {
		Throwable t = ExceptionUtils.getRootCause(e);
		if ( t == null ) t = e;
		if ( t == null ) return;
	    metadata.set(TikaExtractor.TIKA_PARSE_EXCEPTION, t.getClass().getName()+": "+t.getMessage());
	}

	public ContentHandler getHandler( Writer out ) {
		return new WriteOutContentHandler( new ToTextContentHandler( new SpaceTrimWriter(out) ), max_text_length.intValue() );
	}
	
	public class SpaceTrimWriter extends FilterWriter
	{
	  private boolean isStartSpace = true;
	  private boolean lastCharWasSpace;
	  private boolean includedNewline = false;
	  
	  public SpaceTrimWriter(Writer out) { super(out); }
	  public void write(char[] cbuf, int off, int len) throws IOException
	  {
	    for (int i = off; i < len; i++)
	      write(cbuf[ i ]);
	  }
	  public void write(String str, int off, int len) throws IOException
	  {
	    for (int i = off; i < len; i++)
	      write(str.charAt(i));
	  }
	  public void write(int c) throws IOException
	  {
	    if (c == ' ' || c == '\n' || c == '\t') 
	    {
	      lastCharWasSpace = true;
	      if( c == '\n' )
	    	  includedNewline = true;
	    }
	    else
	    {
	      if (lastCharWasSpace)
	      {
	        if (!isStartSpace) {
	        	if( includedNewline ) {
		            out.write('\n');
	        	} else {
	        		out.write(' ');
	        	}
	        }
	        lastCharWasSpace = false;
	        includedNewline = false;
	      }
	      isStartSpace = false;
	      out.write(c);
	    }
	  }
	}
	
	private boolean checkMime( String mime ) {
		if( mime == null )
			return false;

		for( String exclude : excludes ) {
			if( mime.matches( ".*" +  exclude + ".*" ) ) {
				return false;
			}
		}
		return true;
	}
}
