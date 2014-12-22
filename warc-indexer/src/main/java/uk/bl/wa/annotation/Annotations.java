/**
 * 
 */
package uk.bl.wa.annotation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

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

	private static ObjectMapper getObjectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS,
				false);
		return mapper;
	}

	/**
	 * 
	 * @param ann
	 * @return
	 */
	public String toJson() {
		try {
			ObjectMapper mapper = getObjectMapper();
			return mapper.writerWithDefaultPrettyPrinter()
					.writeValueAsString(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param filename
	 * @throws FileNotFoundException
	 */
	public void toJsonFile(String filename) throws FileNotFoundException {
		final OutputStream os = new FileOutputStream(filename);
		final PrintStream printStream = new PrintStream(os);
		printStream.print(this.toJson());
		printStream.close();
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
		ObjectMapper mapper = getObjectMapper();
		return mapper.readValue(json, Annotations.class);
	}

	/**
	 * 
	 * @param filename
	 * @return
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public static Annotations fromJsonFile(String filename)
			throws JsonParseException, JsonMappingException, IOException {
		return fromJson(FileUtils.readFileToString(new File(filename)));
	}

}
