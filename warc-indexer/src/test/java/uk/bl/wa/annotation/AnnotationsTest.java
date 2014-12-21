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
		Annotations ann = new Annotations();
		ann.getCollections()
				.get("root")
				.put("test_key_1", new UriCollection("test1", "test2", "test3"));
		ann.getCollectionDateRanges().put("test_key_2",
				new DateRange(null, null));
		String json = ann.toJson();
		Annotations ann2 = Annotations.fromJson(json);
		String json2 = ann2.toJson();
		// Having performed a full Json-Java-Json cycle, check the Json is the
		// same:
		assertEquals("A serialisation cycle was not lossless", json, json2);
	}

}
