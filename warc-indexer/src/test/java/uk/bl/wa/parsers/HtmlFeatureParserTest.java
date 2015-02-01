/**
 * 
 */
package uk.bl.wa.parsers;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2015 The UK Web Archive
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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import junit.framework.Assert;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.ParseError;
import org.jsoup.parser.Parser;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class HtmlFeatureParserTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void JSoupElementsAddedTest() throws Exception {
		String testHtml = "<b>test<p></b>";
		String baseUri = "http://example.com/dummy.html";
		//
		ByteArrayInputStream stream = new ByteArrayInputStream(
				testHtml.getBytes());
		stream.mark(100000);
		Parser parser = Parser.xmlParser();
		parser.setTrackErrors(1000);
		Document doc = Jsoup.parse(stream, null, baseUri, parser);
		System.out.println("Parsed as:\n" + doc);
		System.out.println("Got " + parser.getErrors().size()
				+ " under isTrackErrors = " + parser.isTrackErrors());
		for (ParseError err : parser.getErrors()) {
			System.out.println("Got: " + err);
		}

		// Also run in through the handler class:
		stream.reset();
		innerBasicParseTest(stream, baseUri, 2);
	}

	private static void printMetadata(Metadata metadata) {
		for (String name : metadata.names()) {
			for (String value : metadata.getValues(name)) {
				System.out.println(name + ": " + value);
			}
		}
	}

	private void innerBasicParseTest(InputStream stream, String uri,
			int numElements)
			throws IOException, SAXException, TikaException {
		HtmlFeatureParser hfp = new HtmlFeatureParser();
		Metadata metadata = new Metadata();
		metadata.set(Metadata.RESOURCE_NAME_KEY, uri);
		hfp.parse(stream, null, metadata, null);
		printMetadata(metadata);
		// for (ParseError err : hfp.getParseErrors()) {
		// System.out.println("PARSE-ERROR: " + err);
		// }
		Assert.assertEquals("Number of distinct elements was wrong,",
				numElements,
				metadata.getValues(HtmlFeatureParser.DISTINCT_ELEMENTS).length );
	}

	/**
	 * Test method for
	 * {@link uk.bl.wa.parsers.HtmlFeatureParser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata, org.apache.tika.parser.ParseContext)}
	 * .
	 * 
	 * @throws Exception
	 */
	@Test
	public void testParseInputStreamContentHandlerMetadataParseContext()
			throws Exception {
		File ml = new File(
				"src/test/resources/wikipedia-mona-lisa/Mona_Lisa.html");
		URL url = ml.toURI().toURL();
		innerBasicParseTest(url.openStream(), url.toString(), 34);
	}

}
