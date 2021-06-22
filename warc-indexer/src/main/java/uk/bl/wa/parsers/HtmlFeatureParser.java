package uk.bl.wa.parsers;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.ParseError;
import org.jsoup.parser.Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.analyser.payload.TikaPayloadAnalyser;
import uk.bl.wa.util.InputStreamUtils;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;

public class HtmlFeatureParser extends AbstractParser {

    /** */
    private static final long serialVersionUID = 1631417895901342814L;

    private static Logger log = LoggerFactory.getLogger(HtmlFeatureParser.class);
    
    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(
                    MediaType.text("html"),
                  MediaType.application("xhtml")
            )));

    // The parser to use, preferring the XML variation as it does not 'fix' the
    // mark-up.
    private Parser parser = Parser.xmlParser();
    // Max errors to returm:
    private int max_errors;
    private final boolean normaliseLinks;
    private boolean extractImageLinks=false;
    
    public static final String ORIGINAL_PUB_DATE = "OriginalPublicationDate";
    // Explicit property to get faster link handling as it allows for set with multiple values (same as LINKS?)
    public static final Property LINK_LIST = Property.internalTextBag("LinkList");
    public static final Property LINKS = Property.internalTextBag("LINK-LIST");
    public static final String FIRST_PARAGRAPH = "FirstParagraph";
    public static final Property IMAGE_LINKS = Property.internalTextBag("ImageLinks");
    public static final Property DISTINCT_ELEMENTS = Property.internalTextBag("DISTINCT-ELEMENTS");
    public static final Property NUM_PARSE_ERRORS = Property
            .internalInteger("Html-Parse-Error-Count");
    public static final int DEFAULT_MAX_PARSE_ERRORS = 1000;

    // Setting this to true also adds the field url_norm to the Solr document in WARCIndexer
    public static final String CONF_LINKS_NORMALISE = "warc.index.extract.linked.normalise";
    public static final String CONF_LINKS_EXTRACT_IMAGE_LINKS = "warc.index.extract.linked.images";
    public static final boolean DEFAULT_LINKS_NORMALISE = false;

    /**
     * 
     */
    public HtmlFeatureParser() {
        this(ConfigFactory.empty());
       }
    public HtmlFeatureParser(Config conf) {
      normaliseLinks = conf.hasPath(CONF_LINKS_NORMALISE) ?
                   conf.getBoolean(CONF_LINKS_NORMALISE) :
                   DEFAULT_LINKS_NORMALISE;
           this.setMaxParseErrors(DEFAULT_MAX_PARSE_ERRORS);
   
           
          extractImageLinks = conf.hasPath(CONF_LINKS_EXTRACT_IMAGE_LINKS) ?
            conf.getBoolean(CONF_LINKS_EXTRACT_IMAGE_LINKS) :
            false;
           
    }

    /**
     * 
     * @param max_errors
     */
    public void setMaxParseErrors(int max_errors) {
        this.max_errors = max_errors;
        parser.setTrackErrors(max_errors);
    }

    /**
     * 
     * @return
     */
    public int getMaxParseErrors() {
        return this.max_errors;
    }
    
    /**
     * 
     * @return
     */
    public List<ParseError> getParseErrors() {
        return this.parser.getErrors();
    }

    /**
     * 
     */
    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    /**
     * 
     */
    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
        final long start = System.nanoTime();
        // Pick up the URL:
        String url = metadata.get( Metadata.RESOURCE_NAME_KEY );
        
        // Parse it using JSoup
        Document doc = null;
        try {
            doc = Jsoup.parse(stream, null, url, parser);
        } catch (java.nio.charset.IllegalCharsetNameException e ) {
            log.warn("Jsoup parse had to assume UTF-8: "+e);
            doc = Jsoup.parse(stream, "UTF-8", url );
        } catch( Exception e ) {
            log.error("Jsoup parse failed: "+e);
        } finally {
            if( doc == null ) return;
        }
        Instrument.timeRel("HTMLAnalyzer.analyze#parser", "HtmlFeatureParser.parse#jsoupparse", start);

        final long nonJsoupStart = System.nanoTime();
        // Record the number of errors found:
        if (parser.getErrors() != null)
            metadata.set(NUM_PARSE_ERRORS, parser.getErrors().size());

        // Get the links (no image links):
        Set<String> links = this.extractLinks(doc);
        if( links != null && links.size() > 0 ) {
            metadata.set(LINK_LIST, links.toArray(new String[links.size()]));
        }
        
        //get the image links
        if (extractImageLinks){
          Set<String> imageLinks = this.extractImageLinks(doc);
          if( imageLinks  != null && imageLinks .size() > 0 ) {
             metadata.set(IMAGE_LINKS, imageLinks.toArray(new String[imageLinks.size()]));
          }
        }
        
        // Get the publication date, from BBC pages:
        for( Element meta : doc.select("meta[name=OriginalPublicationDate]") ) {
            metadata.set(ORIGINAL_PUB_DATE, meta.attr("content"));
            //log.debug(ORIGINAL_PUB_DATE + ": " + meta.attr("content"));
        }
        
        // Grab the first paragraph with text, and extract the text:
        for( Element p : doc.select("p") )  {
            String pt = p.text();
            if( pt != null ) {
                pt = pt.trim();
                if( pt.length() > 0 ) {
                    metadata.set(FIRST_PARAGRAPH, p.text() );
                    //log.debug(FIRST_PARAGRAPH + ": " +p.text() );
                    break;
                }
            }
        }
        
        // Grab the list of distinct elements used in the page:
        Set<String> de = new HashSet<String>();
        for( Element e : doc.select("*") ) {
            // ELEMENT_NAME matching to weed out the worst false positives caused by JavaScript
            // This handles cases such as '<script> if (3<a) console.log('something');' where 'a' would have been
            // seen as a tag, but does not handle cases such as '<script> if ( 3<a ) console.log('something');' where
            // the a is still seen as a tag because it is followed by a space
            if( !"#root".equals(e.tag().getName()) && ELEMENT_NAME.matcher(e.tag().getName()).matches() ) {
                de.add(StringUtils.left(e.tag().getName().toLowerCase(Locale.ENGLISH), 100));
            }
        }
        // For some elements, dig deeper and record attributes too:
        for (Element e : doc.select("link")) {
            if (e.attr("rel") != null) {
                de.add("link/@rel=" + e.attr("rel").toLowerCase());
            }
        }
        for (Element e : doc.select("meta")) {
            if (e.attr("name") != null) {
                de.add("meta/@name=" + e.attr("name").toLowerCase());
            }
        }
        // Store them:
        metadata.set(DISTINCT_ELEMENTS, de.toArray(new String[] {}));
        
        // Licence field, following:
        // http://www.whatwg.org/specs/web-apps/current-work/multipage/links.html#link-type-license
        for( Element a : doc.select("a[rel=license]") )  {
            metadata.add( Metadata.LICENSE_URL, a.attr("href") );
        }
        for( Element a : doc.select("link[rel=license]") )  {
            metadata.add( Metadata.LICENSE_URL, a.attr("href") );
        }
        for( Element a : doc.select("area[rel=license]") )  {
            metadata.add( Metadata.LICENSE_URL, a.attr("href") );
        }
        Instrument.timeRel("HTMLAnalyzer.analyze#parser", "HtmlFeatureParser.parse#featureextract", nonJsoupStart);
    }

    // https://www.w3.org/TR/html/syntax.html#tag-name
    private final Pattern ELEMENT_NAME = Pattern.compile("[a-zA-Z0-9]+");

    /**
     * Use a tolerant parser to extract all of the absolute a href links from a document.
     *
     * Does not extract other links, e.g. stylesheets, etc. etc. Image extracting in another field
     */
    private Set<String> extractLinks( Document doc) throws IOException {
        Set<String> linkset = new HashSet<String>();
        
        // All a with href
        for( Element link : doc.select("a[href]") ) {
            linkset.add( normaliseLink(link.attr("abs:href")));
        }
        

        return linkset;
    }

    // https://www.w3schools.com/TAgs/att_source_srcset.asp
    //  <picture>
    //  <source media="(min-width:650px)" srcset="img_pink_flowers.jpg">
    //  <source media="(min-width:465px)" srcset="img_white_flower.jpg">
    //  <img src="img_orange_flowers.jpg" alt="Flowers" style="width:auto;">
    //</picture>
    //
    // <img src="..."/>
    // <img srcSet="..."/>
    private Set<String> extractImageLinks(Document doc) throws IOException {
        URL base = null;
        try {
            base = doc.location() == null ? null : new URL(doc.location());
        } catch (MalformedURLException e) {
            log.warn("Unable to create base URL for document from '{}'", doc.location());
        }
        Set<String> linkset = new HashSet<String>();
        // HTML 4 body background
        for( Element link : doc.select("body[background]") ) {
            linkset.add( normaliseLink(link.attr("abs:background")));
        }
        // HTML 4 table background
        for( Element link : doc.select("table[background]") ) {
            linkset.add( normaliseLink(link.attr("abs:background")));
        }
        // HTML 4 td background
        for( Element link : doc.select("td[background]") ) {
            linkset.add( normaliseLink(link.attr("abs:background")));
        }
        // Plain img src
        for( Element link : doc.select("img[src]") ) {
            linkset.add( normaliseLink(link.attr("abs:src")));
        }
        // img srcset
        for( Element link : doc.select("img[srcset]") ) {
            linkset.addAll(normaliseSrcsetLinks(base, link.attr("srcset"))); // abs: does not work on multi value
        }
        // picture source srcset
        for (Element links: doc.select("picture source[srcset]")) {
            linkset.addAll(normaliseSrcsetLinks(base, links.attr("srcset")));  // abs: does not work on multi value
        }
        return linkset;
        // Example of use: all PNG references...
        //Elements pngs = doc.select("img[src$=.png]");

        //Element masthead = doc.select("div.masthead").first();
    }


    /**
     * Normalises links if the parser has been configured to do so.
     * @param link a link from a HTML document.
     * @return the link in normalised form or the unchanged link if normalisation has not been enabled.
     */
    public String normaliseLink(String link) {
        return normaliseLinks ? Normalisation.canonicaliseURL(link) : link;
    }

    /**
     * Makes an absolute URL using base if the link is not already absolute, then normalises the URL if the parser has
     * been configured to do so.
     * @param base the base URL for the HTML document.
     * @param link a link from a HTML document.
     * @return the link in normalised form or the unchanged link if normalisation has not been enabled.
     */
    public String normaliseLink(URL base, String link) {
        try {
            link = base == null ? link : new URL(base, link).toString().replace("/../", "/");
        } catch (MalformedURLException e) {
            log.warn("Unable to create absolute URL from new URL('{}', '{}''", base, link);
        }
        return normaliseLinks ? Normalisation.canonicaliseURL(link) : link;
    }

    /**
     * Takes the content of a srcSet, extracts the different images and normalises them.
     * See https://html.com/attributes/img-srcset/
     * @param base the base URL for the HTML document.
     * @param srcSet multi-image attribute.
     * @return the normalised links to images in the srcSet.
     */
    private Set<String> normaliseSrcsetLinks(URL base, String srcSet) {
        if (srcSet == null || srcSet.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> links = new HashSet<>();
        Matcher cm = COMMA_SEPARATED_PATTERN.matcher(srcSet);
        // "foo.jpg 1.5x, bar.jpg 1080w"
        while (cm.find()) {
            final String fullLink = cm.group(1);
            Matcher sm = SPACE_SEPARATED_PATTERN.matcher(fullLink.trim());
            if (sm.matches()) {
                links.add(normaliseLink(base, sm.group(1)));
            } else {
                links.add(normaliseLink(base, fullLink));
            }
        }
        return links;
    }
    private static final Pattern COMMA_SEPARATED_PATTERN = Pattern.compile("([^,]+),?");
   	private static final Pattern SPACE_SEPARATED_PATTERN = Pattern.compile("([^ ]+) ?.*");


    public static Metadata extractMetadata( InputStream in, String url ) {
        HtmlFeatureParser hfp = new HtmlFeatureParser();
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, url);
        try {
            hfp.parse(in, null, metadata, null);
        } catch (Exception e) {
            log.error("Failed to extract Metadata.");
        }
        return metadata;
    }
  
}
