/**
 * 
 */
package uk.bl.wa.annotation;

import java.util.HashMap;

/**
 * 
 * This is the data model for the annotations.
 * 
 * Currently supports Collections and associated date ranges.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class Annotations {

	private HashMap<String, HashMap<String, UriCollection>> collections;
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

}
