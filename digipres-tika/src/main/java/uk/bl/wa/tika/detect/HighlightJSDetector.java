package uk.bl.wa.tika.detect;

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

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.tika.detect.Detector;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.HashMap;

public class HighlightJSDetector implements Detector {
    private static Logger log = LoggerFactory.getLogger(HighlightJSDetector.class.getName());
    
    /**
     * 
     */
    private static final long serialVersionUID = 7717077504684536253L;

    /**
     * The number of bytes from the beginning of the document stream
     * to test for control bytes.
     */
    private static final int DEFAULT_NUMBER_OF_BYTES_TO_TEST = 10*1024;

    private final int bytesToTest;
    
    private ScriptEngineManager manager;
    private ScriptEngine engine;
    
    private static HashMap<String,String> mimeMap;
    
    static {
        mimeMap = new HashMap<String,String>();
        mimeMap.put("unknown", "application/octet-stream");
        mimeMap.put("1c", "application/x-1c");
        mimeMap.put("actionscript", "application/x-actionscript");
        mimeMap.put("apache", "application/x-apache");
        mimeMap.put("avrasm", "application/x-avrasm");
        mimeMap.put("axapta", "application/x-axapta");
        mimeMap.put("bash", "application/x-bash");
        mimeMap.put("coffeescript", "application/x-coffeescript");
        mimeMap.put("cpp", "application/x-cpp");
        mimeMap.put("cs", "application/x-cs");
        mimeMap.put("css", "text/css");
        mimeMap.put("d", "application/x-d");
        mimeMap.put("delphi", "application/x-delphi");
        mimeMap.put("diff", "application/x-diff");
        mimeMap.put("xml", "application/xml");
        mimeMap.put("dos", "application/x-dos");
        mimeMap.put("erlang-repl", "application/x-erlang-repl");
        mimeMap.put("erlang", "application/x-erlang");
        mimeMap.put("glsl", "application/x-glsl");
        mimeMap.put("go", "application/x-go");
        mimeMap.put("haskell", "application/x-haskell");
        mimeMap.put("http", "message/http");// n.b. message/http;msgtype=response or request
        mimeMap.put("ini", "application/x-ini");
        mimeMap.put("java", "application/x-java");
        mimeMap.put("javascript", "application/javascript");
        mimeMap.put("json", "application/x-json");
        mimeMap.put("lisp", "application/x-lisp");
        mimeMap.put("lua", "application/x-lua");
        mimeMap.put("markdown", "application/x-markdown");
        mimeMap.put("matlab", "application/x-matlab");
        mimeMap.put("mel", "application/x-mel");
        mimeMap.put("objectivec", "application/x-objectivec");
        mimeMap.put("perl", "application/x-perl");
        mimeMap.put("php", "application/x-php");
        mimeMap.put("profile", "application/x-profile");
        mimeMap.put("python", "application/x-python");
        mimeMap.put("r", "application/x-r");
        mimeMap.put("rib", "application/x-rib");
        mimeMap.put("rsl", "application/x-rsl");
        mimeMap.put("ruby", "application/x-ruby");
        mimeMap.put("rust", "application/x-rust");
        mimeMap.put("scala", "application/x-scala");
        mimeMap.put("smalltalk", "application/x-smalltalk");
        mimeMap.put("sql", "application/x-sql");
        mimeMap.put("tex", "application/x-tex");
        mimeMap.put("vala", "application/x-vala");
        mimeMap.put("vbscript", "application/x-vbscript");
        mimeMap.put("vhdl", "application/x-vhdl");
        
        // Also add some overrides so that these should be used in the case of text/plain:
        MediaTypeRegistry reg = MediaTypeRegistry.getDefaultRegistry();
        for( String type : mimeMap.values()) {
            if( ! "application/octet-stream".equals(type) ) 
                reg.addSuperType( MediaType.parse(type), MediaType.parse("text/plain") );
        }
    }
    
    /**
     * 
     */
    public HighlightJSDetector() {
        this.bytesToTest = DEFAULT_NUMBER_OF_BYTES_TO_TEST;
        try {
            this.init();
        } catch (ScriptException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void init() throws ScriptException, IOException {
        manager = new ScriptEngineManager();
        engine = manager.getEngineByName("js");
        Reader reader = new InputStreamReader(
                HighlightJSDetector.class.getResourceAsStream("/highlight-rhino.js"), "UTF-8");
        engine.eval(reader);
        reader.close();        
    }
    
    @Override
    public MediaType detect(InputStream input, Metadata metadata)
            throws IOException {
        if (input == null) {
            return MediaType.OCTET_STREAM;
        }
        
        // Default to UTF-8;
        String encoding = "UTF-8";
        
        // Pick up the encoding, if set:
        if( metadata.get( Metadata.CONTENT_ENCODING) != null ) {
            if( Charset.isSupported( metadata.get( Metadata.CONTENT_ENCODING) ) ) {
                encoding = metadata.get( Metadata.CONTENT_ENCODING);
            }
        }

        input.mark(bytesToTest);
        try {
            // Read in and clip to max size:
            final char[] buffer = new char[bytesToTest];
            StringBuilder out = new StringBuilder();
            Reader in = new InputStreamReader( input, encoding);
            try {
              int read;
              // Don't loop - we only want a snippet:
              //do {
                read = in.read(buffer, 0, buffer.length);
                if (read>0) {
                  out.append(buffer, 0, read);
                }
              //} while (read>=0);
            } finally {
              //in.close();
            }
            // Attempt to identify:
            //System.out.println("GOT "+out.toString());
            HljsResult result = this.identify( out.toString() );
            //log.info("for "+metadata.get( Metadata.CONTENT_TYPE)+ " got "+result.getLanguage());
            // Map to MIME type:
            if ( mimeMap.containsKey(result.getLanguage()) ) {
                MediaType mt = MediaType.parse(mimeMap.get(result.getLanguage()));
                metadata.set("X-HLJS-Content-Type", mt.toString());
                return mt;
            } else {
                return MediaType.OCTET_STREAM;
            }
        } catch ( Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            
        } finally {
            input.reset();
        }
        
        // If something went wrong, assume nothing:
        return MediaType.OCTET_STREAM;
    }
    

    
    public class HljsResult {
        private String language;
        private int relevance;

        public HljsResult(String language, int relevance) {
            this.language = language;
            this.relevance = relevance;
        }

        /**
         * @return the language
         */
        public String getLanguage() {
            return language;
        }

        /**
         * @return the relevance
         */
        public int getRelevance() {
            return relevance;
        }
        
        /**
         * 
         */
        public String toString() {
            return language+"["+relevance+"]";
        }
    }
    
    private HljsResult getHljsResult(String var) throws ScriptException {
        try {
            engine.eval("language = "+var+".language;");
            String language = (String) engine.get("language");

            engine.eval("relevance = "+var+".relevance;");
            int relevance = ((Double) engine.get("relevance")).intValue();
        
            // Don't bother setting up the keyword count or highlighted text as this is not of interest here.
            //engine.eval("keyword_count = "+var+".keyword_count;");
            //double keyword_count = (Double) engine.get("keyword_count");
            //engine.eval("text = "+var+".text;");
            
            return new HljsResult(language, relevance);
            
        } catch( Exception e ) {
            return new HljsResult("unknown", 0);
        }
    }
    
    /**
     * 
     * @see http://softwaremaniacs.org/wiki/doku.php/highlight.js:api
     * 
     * @param snippet
     * @return
     * @throws ScriptException
     */
    public HljsResult identify(String snippet) throws ScriptException {
        engine.put("snippet", snippet);
        engine.eval("hlid = hljs.highlightAuto(snippet)");
        
        // Extract the best match:
        HljsResult best = this.getHljsResult("hlid");
        
        // Extract the second-best match:
        HljsResult second_best = this.getHljsResult("hlid.second_best");
        
        log.info("Identified: "+best+", "+second_best);
        return best;
    }
    
    
    
        public static void main(String[] args) {

            try {
                HighlightJSDetector hljs = new HighlightJSDetector();
                System.out.println("GOT: "+hljs.detect(new ByteArrayInputStream("<xml></xml>".getBytes()), new Metadata()) );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
}
