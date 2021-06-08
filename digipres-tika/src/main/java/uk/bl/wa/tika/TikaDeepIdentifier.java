/**
 * 
 */
package uk.bl.wa.tika;

/*
 * #%L
 * digipres-tika
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import org.apache.commons.io.IOUtils;
import org.apache.tika.detect.CompositeDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;



/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TikaDeepIdentifier {
    
    private static Logger log = LoggerFactory.getLogger(TikaDeepIdentifier.class.getName());
    
    private static int MAX_BUF = 1024*1024;

    // Number of milliseconds before timing out. Defaults to 5 mins (5*60*1000 = 300,000 milliseconds).
    private final long parseTimeout = 5*60*1000L;

    // Abort handler, limiting the output size, to avoid OOM:
    private static WriteOutContentHandler ch = null;
    // Silent handler:
    //ContentHandler ch = new DefaultHandler();
    
    // Set up a parse context:
    private static ParseContext ctx = new ParseContext();
    
    // Set up the parser:
    private static PreservationParser pika = new PreservationParser();    
    static {
        pika.init(ctx);
    }
    
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        TikaDeepIdentifier tdi = new TikaDeepIdentifier();
        tdi.printDetectors();
        System.out.println("GOT: "+tdi.identify(new byte[] {'%','P','D','F','-'}));
    }
    
    public void printDetectors() {
        CompositeDetector ds = (CompositeDetector) pika.getDetector();
        for( Detector d : ds.getDetectors()) {
            System.out.println("Detector: "+d.getClass().getCanonicalName());
        }
        for( MediaType type : pika.getParsers().keySet()) {
            System.out.println("Parser: "+type+" : "+pika.getParsers().get(type).getClass());
        }
    }

    /**
     * 
     * @param payload
     * @return
     */
    public String identify(byte[] payload) {
        // Fallback
        String tikaType = MediaType.OCTET_STREAM.toString();
        // Set up metadata object:
        Metadata md = new Metadata();
        TikaInputStream tis = null;
        try {
            tis = TikaInputStream.get( payload, md );
            // Type according to Tiki:
            tikaType = pika.getDetector().detect( tis, md ).toString();
        } catch( Throwable e ) {
            log.error( "Tika.detect failed:" + e.getMessage() );
            //e.printStackTrace();
            return MediaType.OCTET_STREAM.toString();
        } finally {
            if (tis != null) {
                try {
                    tis.close();
                } catch (IOException e) {
                    log.warn("Exception closing TikaInputStream. This leaves tmp-files: " +  e.getMessage());
                }
            }
        }

        // Now perform full parse, to find a more detailed tikaType
        try {
            // Default to detected MIME Type:
            md.set( Metadata.CONTENT_TYPE, tikaType.toString() );
            
            // Ensure parsing is NOT recursive:
            pika.setRecursive(ctx, false);
            
            // Now perform the parsing:
            //parser.parse( new ByteArrayInputStream( payload ), ch, md, ctx );
            // One could forcibly limit the size if OOM is still causing problems, like this:
            //parser.parse( new ByteArrayInputStream( value.getPayload(), 0, BUF_8KB ), ch, md, ctx );

            // Every resource gets it's own write-out buffer:
            ch = new WriteOutContentHandler(MAX_BUF);

            // Run the parser in a separate thread:
            InputStream tikainput = TikaInputStream.get( payload, md );
            ParseRunner runner = new ParseRunner( pika, tikainput, ch, md, ctx );
            Thread parseThread = new Thread( runner, Long.toString( System.currentTimeMillis() ) );
            parseThread.setDaemon(true); // Daemon to ensure proper shutdown when overall processing has finished

            try {
                // TODO: This should use TimeLimiter.run(parser, 30000L, false); but that is in the warc-indexer module
                parseThread.start();
                parseThread.join( this.parseTimeout );
                parseThread.interrupt();
            } catch( OutOfMemoryError o ) {
                log.error( "TikaExtractor.parse(): " + tikaType + " : " + o.getMessage() );
            } catch( RuntimeException r ) {
                log.error( "TikaExtractor.parse(): " + tikaType + " : " + r.getMessage() );
            } finally {
                if (tikainput != null) {
                    try {
                        tikainput.close();
                    } catch (IOException e) {
                        log.warn("Exception closing TikaInputStream. This leaves tmp-files: " +  e.getMessage());
                    }
                }
            }
            
            // Use the extended MIME type generated by the PreservationParser:
            String extMimeType = md.get(PreservationParser.EXT_MIME_TYPE);
            if( runner.complete && extMimeType != null ) tikaType = extMimeType;
            
        } catch( Throwable e ) {
            log.debug( "Tika Exception: " + e.getMessage() );
            //e.printStackTrace();
        }
        // Return whichever value works:
        return tikaType;
    }
    
    /**
     * 
     * @param payload
     * @param metadata 
     * @return
     */
    public String identify(InputStream payload, Metadata metadata) {
        // Fallback
        String tikaType = MediaType.OCTET_STREAM.toString();
        // Set up metadata object:
        Metadata md = metadata;
        TikaInputStream tis = null;
        try {
            tis = TikaInputStream.get( payload );
            // Type according to Tiki:
            tikaType = pika.getDetector().detect( tis, md ).toString();
        } catch( Throwable e ) {
            log.error( "Tika.detect failed:" + e.getMessage() );
            //e.printStackTrace();
            return MediaType.OCTET_STREAM.toString();
        } finally {
            if (tis != null) {
                try {
                    tis.close();
                } catch (IOException e) {
                    log.warn("Exception closing TikaInputStream. This leaves tmp-files: " +  e.getMessage());
                }
            }
        }

        // Now perform full parse, to find a more detailed tikaType
        try {
            // Default to detected MIME Type:
            md.set( Metadata.CONTENT_TYPE, tikaType.toString() );
            
            // Ensure parsing is NOT recursive:
            pika.setRecursive(ctx, false);
            
            // Now perform the parsing:
            //parser.parse( new ByteArrayInputStream( payload ), ch, md, ctx );
            // One could forcibly limit the size if OOM is still causing problems, like this:
            //parser.parse( new ByteArrayInputStream( value.getPayload(), 0, BUF_8KB ), ch, md, ctx );

            // Every resource gets it's own write-out buffer:
            ch = new WriteOutContentHandler(MAX_BUF);
            
            // Run the parser in a separate thread:
            InputStream tikainput = TikaInputStream.get( payload );
            ParseRunner runner = new ParseRunner( pika, tikainput, ch, md, ctx );
            Thread parseThread = new Thread( runner, Long.toString( System.currentTimeMillis() ) );
            parseThread.setDaemon(true); // Daemon to ensure proper shutdown when overall processing has finished
            try {
                // TODO: This should use TimeLimiter.run(parser, 30000L, false); but that is in the warc-indexer module
                parseThread.start();
                parseThread.join( this.parseTimeout );
                parseThread.interrupt();
            } catch( OutOfMemoryError o ) {
                log.error( "TikaExtractor.parse(): " + tikaType + " : " + o.getMessage() );
            } catch( RuntimeException r ) {
                log.error( "TikaExtractor.parse(): " + tikaType + " : " + r.getMessage() );
            } finally {
                if (tikainput != null) {
                    try {
                        tikainput.close();
                    } catch (IOException e) {
                        log.warn("Exception closing TikaInputStream. This leaves tmp-files: " +  e.getMessage());
                    }
                }
            }

            // Use the extended MIME type generated by the PreservationParser:
            String extMimeType = md.get(PreservationParser.EXT_MIME_TYPE);
            if( runner.complete && extMimeType != null ) tikaType = extMimeType;
            
        } catch( Throwable e ) {
            log.debug( "Tika Exception: " + e.getMessage() );
            //e.printStackTrace();
        }
        // Return whichever value works:
        return tikaType;
    }

    private File copyToTempFile( String name, byte[] content, int max_bytes ) throws Exception {
        File tmp = File.createTempFile("FmtTmp-", name);
        tmp.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tmp);
        IOUtils.copy(new ByteArrayInputStream(content, 0, max_bytes), fos);
        fos.flush();
        fos.close();
        return tmp;
    }
    
    private File copyToTempFile( String name, byte[] content ) throws Exception {
        //if( content.length < BUF_8KB )
        return copyToTempFile(name, content, MAX_BUF);
    }
    
    // ----
    
    private class ParseRunner implements Runnable {
        private AutoDetectParser parser;
        private InputStream tikainput;
        private ContentHandler handler;
        private Metadata metadata;
        private ParseContext context;
        public boolean complete;

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
    
    // --- Thread pool example, running external command ---
    
    // private static final ExecutorService THREAD_POOL
    // = Executors.newCachedThreadPool();
    //
    // private static <T> T timedCall(Callable<T> c, long timeout, TimeUnit
    // timeUnit)
    // throws InterruptedException, ExecutionException, TimeoutException
    // {
    // FutureTask<T> task = new FutureTask<T>(c);
    // THREAD_POOL.execute(task);
    // return task.get(timeout, timeUnit);
    // }
    //
//    void then() throws InterruptedException, ExecutionException {
//        int timeout = 10;
//        try {
//            int returnCode = timedCall(new Callable<Integer>() {
//                public Integer call() throws Exception
//                {
//                    java.lang.Process process = Runtime.getRuntime().exec("command"); 
//                    return process.waitFor();
//                }}, new Integer(timeout), TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            // Handle timeout here
//        }
//    }
    //
}
