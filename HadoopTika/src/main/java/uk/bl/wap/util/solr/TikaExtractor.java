package uk.bl.wap.util.solr;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_URI;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Properties;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.xml.sax.ContentHandler;

import uk.bl.wap.util.solr.SolrRecord;
import uk.bl.wap.util.warc.WARCRecordUtils;

public class TikaExtractor {
	private static final String CONFIG = "/hadoop_utils.config";
	private long parseTimeout = 300000L;
	private String[] excludes;
	private Tika tika;

	public TikaExtractor() {
		this.tika = new Tika();
		Properties properties = new Properties();
		try {
			properties.load( this.getClass().getResourceAsStream( ( CONFIG ) ) );
			this.excludes = properties.getProperty( "mime_exclude" ).split( "," );
			this.parseTimeout = Long.parseLong( properties.getProperty( "parse_timeout" ) );
		} catch( IOException i ) {
			System.err.println( "Could not find Properties file: " + i.getMessage() );
		}
	}

	public SolrRecord extract( byte[] payload ) {
		SolrRecord solr = new SolrRecord();

		if( !this.checkMime( tika.detect( payload ) ) ) {
			return solr;
		}

		ParseContext context;
		Detector detector;
		AutoDetectParser parser;
		Metadata metadata = new Metadata();
		ByteArrayOutputStream content = new ByteArrayOutputStream();
		context = new ParseContext();
		try {
			detector = ( new TikaConfig() ).getMimeRepository();
		} catch( Exception i ) {
			return solr;

		}
		parser = new AutoDetectParser( detector );
		context.set( Parser.class, parser );

		try {
			InputStream tikainput = TikaInputStream.get( payload, metadata );
			ParseRunner runner = new ParseRunner( parser, tikainput, this.getHandler( content ), metadata, context );
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
			String output;
			if( runner.complete || !content.toString( "UTF-8" ).equals( "" ) ) {
				output = content.toString( "UTF-8" ).replaceAll( "<!\\[CDATA\\[", "" );
				output = output.toString().replaceAll( "\\]\\]>", "" );
				solr.setExtractedText( output );
			}
			solr.setContentType( metadata.get( "Content-Type" ) );

			if( metadata.get( DublinCore.TITLE ) != null )
				solr.setTitle( metadata.get( DublinCore.TITLE ).trim().replaceAll( "\\p{Cntrl}", "" ) );
			if( metadata.get( DublinCore.DESCRIPTION ) != null )
				solr.setDescription( metadata.get( DublinCore.DESCRIPTION ).trim().replaceAll( "\\p{Cntrl}", "" ) );
		} catch( Exception e ) {
			System.err.println( "TikaExtractor.extract(): " + e.getMessage() );
		}
		return solr;
	}

	private class ParseRunner implements Runnable {
		private AutoDetectParser parser;
		private InputStream tikainput;
		private ContentHandler handler;
		private Metadata metadata;
		private ParseContext context;
		private boolean complete;

		public ParseRunner( AutoDetectParser parser, InputStream tikainput, ContentHandler handler, Metadata metadata, ParseContext context ) {
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
			} catch( Exception e ) {
				System.err.println( "ParseRunner.run(): " + e.getMessage() );
			}
		}
	}

	public ContentHandler getHandler( OutputStream out ) throws TransformerConfigurationException {
		SAXTransformerFactory factory = ( SAXTransformerFactory ) SAXTransformerFactory.newInstance();
		TransformerHandler handler = factory.newTransformerHandler();
		handler.getTransformer().setOutputProperty( OutputKeys.METHOD, "text" );
		handler.getTransformer().setOutputProperty( OutputKeys.INDENT, "yes" );
		handler.getTransformer().setOutputProperty( OutputKeys.ENCODING, "UTF-8" );
		handler.setResult( new StreamResult( out ) );
		return handler;
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
