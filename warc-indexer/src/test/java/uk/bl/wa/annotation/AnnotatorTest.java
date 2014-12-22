package uk.bl.wa.annotation;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.commons.httpclient.URIException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;

public class AnnotatorTest {

	private Annotations annotations;
	private Annotator annotator;

	@Before
	public void setUp() throws Exception {
		annotations = Annotations
				.fromJsonFile("src/test/resources/test-annotations.json");
		annotator = new Annotator(annotations);
	}

	@Test
	public void testApplyAnnotations() throws URIException, URISyntaxException {
		//
		URI uri = URI.create("http://en.wikipedia.org/wiki/Mona_Lisa");
		//
		SolrInputDocument solr = new SolrInputDocument();
		Date d = Calendar.getInstance().getTime();
		solr.setField(SolrFields.CRAWL_DATE, WARCIndexer.formatter.format(d));
		solr.setField(SolrFields.SOLR_URL, uri);
		//
		annotator.applyAnnotations(uri, solr);
		System.out.println("SOLR: " + solr.toString());
		int found = 0;
		//
		//
		for (Object val : solr.getFieldValues(SolrFields.SOLR_COLLECTIONS)) {
			@SuppressWarnings("unchecked")
			Map<String, String> cmap = (Map<String, String>) val;
			for (String item : cmap.values()) {
				System.out.println("Map contains... " + item);
				if ("Wikipedia".equals(item))
					found++;
				if ("Wikipedia|Main Site".equals(item))
					found++;
				if ("Wikipedia|Main Site|Mona Lisa".equals(item))
					found++;
			}
		}
		assertTrue("Can't find expected entries in 'collections.", found >= 3);
	}

}
