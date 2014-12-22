/**
 * 
 */
package uk.bl.wa.annotation;

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

	@Test
	public void testJsonSerialisation() throws JsonParseException,
			JsonMappingException, IOException {
		//
		Annotations ann = new Annotations();
		//
		ann.getCollections()
				.get("resource")
				.put("en.wikipedia.org/wiki/Mona_Lisa",
						new UriCollection("Wikipedia", new String[] {
								"Wikipedia", "Wikipedia|Main Site",
								"Wikipedia|Main Site|Mona Lisa" },
								new String[] { "Crowdsourcing" }));
		//
		ann.getCollections()
				.get("root")
				.put("http://en.wikipedia.org/",
						new UriCollection("Wikipedia", new String[] {
								"Wikipedia", "Wikipedia|Main Site" },
								new String[] { "Crowdsourcing" }));
		//
		ann.getCollections()
				.get("subdomains")
				.put("http://en.wikipedia.org",
						new UriCollection("Wikipedia",
								new String[] { "Wikipedia" },
								new String[] { "Crowdsourcing" }));
		// Date ranges:
		ann.getCollectionDateRanges().put("Wikipedia",
				new DateRange(null, null));
		String json = ann.toJson();
		Annotations ann2 = Annotations.fromJson(json);
		ann2.toJsonFile("src/test/resources/test-annotations.json");
		String json2 = ann2.toJson();
		// Having performed a full Json-Java-Json cycle, check the Json is the
		// same:
		assertEquals("A serialisation cycle was not lossless", json, json2);
	}

}
