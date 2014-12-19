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

	public Annotations(
			HashMap<String, HashMap<String, UriCollection>> collections2,
			HashMap<String, DateRange> collectionDateRanges2) {
		this.collections = collections2;
		this.collectionDateRanges = collectionDateRanges2;
	}

	public HashMap<String, HashMap<String, UriCollection>> getCollections() {
		return this.collections;
	}

	public HashMap<String, DateRange> getCollectionDateRanges() {
		return this.collectionDateRanges;
	}

}
