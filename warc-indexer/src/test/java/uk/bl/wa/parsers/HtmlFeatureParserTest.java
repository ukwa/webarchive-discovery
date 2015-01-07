/**
 * 
 */
package uk.bl.wa.parsers;

import static org.junit.Assert.fail;

import java.net.URL;

import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.Test;

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
		URL url = new URL("http://www.bbc.co.uk/news/magazine-21351017");
		// url = new URL(
		// "http://labs.creativecommons.org/2011/ccrel-guide/examples/moremetadata.html");
		HtmlFeatureParser hfp = new HtmlFeatureParser();
		Metadata metadata = new Metadata();
		metadata.set(Metadata.RESOURCE_NAME_KEY, url.toString());
		hfp.parse(url.openStream(), null, metadata, null);
		System.out.println("RESULT: " + metadata);
		fail("Not yet implemented");
	}

}
