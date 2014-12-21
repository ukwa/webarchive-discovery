package uk.bl.wa.annotation;

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
	protected String collectionCategories;

	@JsonProperty
	protected String[] allCollections;

	@JsonProperty
	protected String[] subject;
	
	protected UriCollection() {
	}

	public UriCollection(String collectionCategories, String allCollections,
			String subject) {
		if (collectionCategories != null && collectionCategories.length() > 0)
			this.collectionCategories = collectionCategories;
		if (allCollections != null && allCollections.length() > 0)
			this.allCollections = allCollections.split("\\s*\\|\\s*");
		if (subject != null && subject.length() > 0)
			this.subject = subject.split("\\s*\\|\\s*");
	}
}
