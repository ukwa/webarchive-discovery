/**
 * 
 */
package uk.bl.wa.annotation;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * This is the data model for the annotations.
 * 
 * The collections hash-map collects the URI-to-Collection maps by scope, i.e.
 * HashMap<SCOPE, HashMap<URI, COLLECTION>>
 * 
 * The collectionDateRanges refines this by noting the date restriction for each
 * top-level collection.
 * 
 * Currently supports Subjects, Collections and associated date ranges.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class Annotations {

	@JsonProperty
	private HashMap<String, HashMap<String, UriCollection>> collections;

	@JsonProperty
	private HashMap<String, DateRange> collectionDateRanges;

	public Annotations() {
		// Initialise the collection maps:
		collections = new HashMap<String, HashMap<String, UriCollection>>();
		collections.put("resource", new HashMap<String, UriCollection>());
		collections.put("plus1", new HashMap<String, UriCollection>());
		collections.put("root", new HashMap<String, UriCollection>());
		collections.put("subdomains", new HashMap<String, UriCollection>());

		// An the date ranges:
		collectionDateRanges = new HashMap<String, DateRange>();
	}

	public HashMap<String, HashMap<String, UriCollection>> getCollections() {
		return this.collections;
	}

	public HashMap<String, DateRange> getCollectionDateRanges() {
		return this.collectionDateRanges;
	}

	/**
	 * 
	 * @param ann
	 * @return
	 */
	public String toJson() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param json
	 * @return
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public static Annotations fromJson(String json) throws JsonParseException,
			JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		// TypeFactory typeFactory = mapper.getTypeFactory();
		// MapType mapType = typeFactory.constructMapType(HashMap.class,
		// String.class, DateRange.class);
		// HashMap<String, DateRange> map = mapper.readValue(json, mapType);
		return mapper.readValue(json, Annotations.class);
	}

}
