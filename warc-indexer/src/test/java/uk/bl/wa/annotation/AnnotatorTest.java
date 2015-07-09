package uk.bl.wa.annotation;

/*
 * #%L
 * warc-indexer
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

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

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
		annotations = Annotations.fromJsonFile(AnnotationsTest.ML_ANNOTATIONS);
		annotator = new Annotator(annotations);
	}

	@Test
	public void testApplyAnnotations() throws URIException, URISyntaxException {
		innerTestApplyAnnotations("http://en.wikipedia.org/wiki/Mona_Lisa", 3);
		innerTestApplyAnnotations("http://en.wikipedia.org/", 2);
		innerTestApplyAnnotations("http://www.wikipedia.org/", 1);
	}

	private void innerTestApplyAnnotations(String uriString, int expected)
			throws URIException, URISyntaxException {
		//
		URI uri = URI.create(uriString);
		//
		// Get the calendar instance.
		Calendar calendar = Calendar.getInstance();
		// Set the time for the notification to occur.
		calendar.set(Calendar.YEAR, 2013);
		calendar.set(Calendar.MONTH, 6);
		calendar.set(Calendar.DAY_OF_MONTH, 17);
		calendar.set(Calendar.HOUR_OF_DAY, 10);
		calendar.set(Calendar.MINUTE, 45);
		calendar.set(Calendar.SECOND, 0);
		calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
		//
		SolrInputDocument solr = new SolrInputDocument();
		Date d = calendar.getTime();
		solr.setField(SolrFields.CRAWL_DATE, WARCIndexer.formatter.format(d));
		solr.setField(SolrFields.SOLR_URL, uri);
		//
		annotator.applyAnnotations(uri, solr);
		Annotator.prettyPrint(System.out, solr);
		int found = 0;
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
		assertTrue("Can't find the " + expected
				+ " expected entries in 'collections for " + uriString,
				found == expected);
	}

}
