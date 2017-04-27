/**
 * 
 */
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class AnnotationsTest {

    public static final String ML_ANNOTATIONS = "src/test/resources/annotations/mona-lisa-annotations.json";
    public static final String ML_OASURTS = "src/test/resources/annotations/openAccessSurts.txt";

	@Test
	public void testJsonSerialisation() throws JsonParseException,
			JsonMappingException, IOException {
		//
		Annotations ann = new Annotations();
		// canon.urlStringToKey(uri)
		ann.getCollections()
				.get("resource")
				.put("http://en.wikipedia.org/wiki/Mona_Lisa",
						new UriCollection("Wikipedia", new String[] {
								"Wikipedia", "Wikipedia|Main Site",
								"Wikipedia|Main Site|Mona Lisa" },
								new String[] { "Crowdsourcing" }));
		//
		ann.getCollections()
				.get("root")
				.put("http://en.wikipedia.org",
						new UriCollection("Wikipedia", new String[] {
								"Wikipedia", "Wikipedia|Main Site" },
								new String[] { "Crowdsourcing" }));
		//
		ann.getCollections()
				.get("subdomains")
				.put("en.wikipedia.org",
						new UriCollection("Wikipedia",
								new String[] { "Wikipedia" },
								new String[] { "Crowdsourcing" }));
		// Date ranges:
		ann.getCollectionDateRanges().put("Wikipedia",
				new DateRange(null, null));
		ann.getCollectionDateRanges().put("Wikipedia|Main Site",
				new DateRange(null, null));
		ann.getCollectionDateRanges().put("Wikipedia|Main Site|Mona Lisa",
				new DateRange(null, null));
		String json = ann.toJson();
		Annotations ann2 = Annotations.fromJson(json);
		ann2.toJsonFile(ML_ANNOTATIONS);
		String json2 = ann2.toJson();
		// Having performed a full Json-Java-Json cycle, check the Json is the
		// same:
		assertEquals(
				"The test Json-Java-Json serialisation cycle was not lossless",
				json, json2);
	}

}
