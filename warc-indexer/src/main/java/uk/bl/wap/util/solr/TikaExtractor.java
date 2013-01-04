package uk.bl.wap.util.solr;

import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.tika.Tika;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.ContentHandler;

public class TikaExtractor {
	private long parseTimeout;
	private String[] excludes;
	private Tika tika;
	
	/** Maximum number of characters of text to pull out of any given resource: */
	private int MAX_TEXT_LENGTH = 1024*1024; // 1MB ~= 1024 * 1KB

	public TikaExtractor() {
		this( new Configuration() );
	}

	public TikaExtractor( Configuration conf ) {
		this.tika = new Tika();
		this.excludes = conf.getStrings( "tika.exclude.mime", new String[ 0 ] );
		this.parseTimeout = conf.getLong( "tika.timeout", 300000L );
	}

	public WritableSolrRecord extract( WritableSolrRecord solr, InputStream is, String url ) throws IOException {
		
		TikaInputStream tikainput = TikaInputStream.get(is);
		// Also pass URL as metadata to allow extension hints to work:
		Metadata metadata = new Metadata();
		metadata.set( Metadata.RESOURCE_NAME_KEY, url);
		String detected = tika.detect( tikainput, metadata );
		// Only proceed if we have a suitable type:
		if( !this.checkMime( detected ) ) {
			return solr;
		}

		ParseContext context = new ParseContext();
		StringWriter content = new StringWriter();

		try {
			ParseRunner runner = new ParseRunner( tika.getParser(), tikainput, this.getHandler( content ), metadata, context );
			Thread parseThread = new Thread( runner, Long.toString( System.currentTimeMillis() ) );
			try {
				parseThread.start();
				parseThread.join( this.parseTimeout );
				parseThread.interrupt();
			} catch( OutOfMemoryError o ) {
				System.err.println( "TikaExtractor.parse(): " + o.getMessage() );
			} catch( RuntimeException r ) {
				System.err.println( "TikaExtractor.parse(): " + r.getMessage() );
			}
			String output = content.toString();
			if( runner.complete || !output.equals( "" ) ) {
				solr.doc.setField( SolrFields.SOLR_EXTRACTED_TEXT, output );
				solr.doc.setField( SolrFields.SOLR_EXTRACTED_TEXT_LENGTH, Integer.toString( output.length() ) );
			}
			
			for( String m : metadata.names() ) {
				System.err.println("Metadata: "+m+" -> "+metadata.get(m));
			}
			
			solr.doc.setField( SolrFields.SOLR_CONTENT_TYPE, metadata.get( Metadata.CONTENT_TYPE ) );

			if( metadata.get( DublinCore.TITLE ) != null )
				solr.doc.setField( SolrFields.SOLR_TITLE, metadata.get( DublinCore.TITLE ).trim().replaceAll( "\\p{Cntrl}", "" ) );
			if( metadata.get( DublinCore.DESCRIPTION ) != null )
				solr.doc.setField( SolrFields.SOLR_DESCRIPTION, metadata.get( DublinCore.DESCRIPTION ).trim().replaceAll( "\\p{Cntrl}", "" ) );
		} catch( Exception e ) {
			System.err.println( "TikaExtractor.extract(): " + e.getMessage() );
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
				System.err.println( "ParseRunner.run(): " + i.getMessage() );
			} catch( Exception e ) {
				System.err.println( "ParseRunner.run(): " + e.getMessage() );
			}
		}
	}

	public ContentHandler getHandler( Writer out ) {
		return new WriteOutContentHandler( new ToTextContentHandler( new SpaceTrimWriter(out) ), MAX_TEXT_LENGTH );
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
		for( String exclude : excludes ) {
			if( mime.matches( ".*" +  exclude + ".*" ) ) {
				return false;
			}
		}
		return true;
	}
}
