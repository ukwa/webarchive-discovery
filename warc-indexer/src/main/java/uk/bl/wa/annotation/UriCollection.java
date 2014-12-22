package uk.bl.wa.annotation;

import java.util.Arrays;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 * Part of the @Annotations data model.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class UriCollection {

	@JsonProperty
	protected String collection;

	@JsonProperty
	protected String[] collections;

	@JsonProperty
	protected String[] subject;
	
	protected UriCollection() {
	}

	public UriCollection(String collection, String[] collections,
			String[] subjects) {
		this.collection = collection;
		this.collections = collections;
		this.subject = subjects;
	}

	public UriCollection(String collection, String collections,
			String subject) {
		if (collection != null && collection.length() > 0)
			this.collection = collection;
		if (collections != null && collections.length() > 0)
			this.collections = collections.split("\\s*\\|\\s*");
		if (subject != null && subject.length() > 0)
			this.subject = subject.split("\\s*\\|\\s*");
	}

	public String toString() {
		return collection + " : " + Arrays.toString(collections) + " : "
				+ Arrays.toString(subject);
	}
}
