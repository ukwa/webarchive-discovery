/**
 * 
 */
package uk.bl.wa.solr;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TikaExtractorTest {

	private TikaExtractor tika;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		tika = new TikaExtractor();
	}

	@Test
	public void testMonaLisa() throws Exception {
		File ml = new File(
				"src/test/resources/wikipedia-mona-lisa/Mona_Lisa.html");
		URL url = ml.toURI().toURL();
		SolrRecord solr = new SolrRecord();
		tika.extract(solr, url.openStream(), url.toString());
		System.out.println("SOLR " + solr.getSolrDocument().toString());
		String text = (String) solr.getField(SolrFields.SOLR_EXTRACTED_TEXT)
				.getValue();
		assertTrue("Text should contain this string!",
				text.contains("Mona Lisa"));
		assertFalse(
				"Text should NOT contain this string! (implies bad newline handling)",
				text.contains("encyclopediaMona"));
	}

}
