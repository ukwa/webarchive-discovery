package uk.bl.wa.annotation;

public class UriCollection {
	protected String collectionCategories;
	protected String[] allCollections;
	protected String[] subject;

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
