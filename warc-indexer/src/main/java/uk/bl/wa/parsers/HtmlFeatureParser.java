package uk.bl.wa.parsers;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2014 The UK Web Archive
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import uk.bl.wa.util.Instrument;

public class HtmlFeatureParser extends AbstractParser {

	/** */
	private static final long serialVersionUID = 1631417895901342813L;

	private static Log log = LogFactory.getLog(HtmlFeatureParser.class);
	
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

	public static final String ORIGINAL_PUB_DATE = "OriginalPublicationDate";
    // Explicit property to get faster link handling as it allows for set with multiple values (same as LINKS?)
    public static final Property LINK_LIST = Property.internalTextBag("LinkList");
	public static final Property LINKS = Property.internalTextBag("LINK-LIST");
	public static final String FIRST_PARAGRAPH = "FirstParagraph";
	public static final Property DISTINCT_ELEMENTS = Property.internalTextBag("DISTINCT-ELEMENTS");
	public static final Property NUM_PARSE_ERRORS = Property
			.internalInteger("Html-Parse-Error-Count");
	public static final int DEFAULT_MAX_PARSE_ERRORS = 1000;

	/**
	 * 
	 */
	public HtmlFeatureParser() {
		this.setMaxParseErrors(DEFAULT_MAX_PARSE_ERRORS);
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
		Set<String> links = this.extractLinks(doc, false);
		if( links != null && links.size() > 0 ) {
			metadata.set(LINK_LIST, links.toArray(new String[links.size()]));
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
			if( !"#root".equals(e.tag().getName()) )
				de.add(StringUtils.left(e.tag().getName(), 100));
		}
		// For some elements, dig deeper and record attributes too:
		for (Element e : doc.select("link")) {
			de.add("link/@rel=" + e.attr("rel"));
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
	

	/**
	 * Use a tolerant parser to extract all of the absolute a href links from a document.
	 * 
	 * Does not extract other links, e.g. stylesheets, etc. etc. Image links optional.
	 * 
	 * @param input The InputStream
	 * @param charset The character set, e.g. "UTF-8". Value of "null" attempts to extract encoding from the document and falls-back on UTF-8.
	 * @param baseUri base URI for the page, for resolving relative links. e.g. "http://www.example.com/"
	 * @return
	 * @throws IOException
	 */
	private Set<String> extractLinks( Document doc, boolean includeImgLinks ) throws IOException {
		Set<String> linkset = new HashSet<String>();
		
		// All a with href
		for( Element link : doc.select("a[href]") ) {
			linkset.add(link.attr("abs:href"));
		}
		// All images:
		if( includeImgLinks ) {
			for( Element link : doc.select("img[src]") ) {
				linkset.add( link.attr("abs:src") );
			}
		}
		// Example of use: all PNG references...
		//Elements pngs = doc.select("img[src$=.png]");

		//Element masthead = doc.select("div.masthead").first();
		return linkset;
	}

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
