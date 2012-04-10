package uk.bl.wap.util.solr;

public interface SolrFields {
	public static final String SOLR_ID = "id";
	public static final String SOLR_URL = "wct_url";
	public static final String SOLR_DOMAIN = "wct_domain";
	public static final String SOLR_DIGEST = "hash";
	public static final String SOLR_TITLE = "title";
	public static final String SOLR_SUBJECT = "subject";
	public static final String SOLR_DESCRIPTION = "description";
	public static final String SOLR_COMMENTS = "comments";
	public static final String SOLR_AUTHOR = "author";
	public static final String SOLR_KEYWORDS = "keywords"; 
	public static final String SOLR_CATEGORY = "category";
	public static final String SOLR_CONTENT_TYPE = "content_type";
	public static final String SOLR_NORMALISED_CONTENT_TYPE = "content_type_norm";
	public static final String SOLR_TIMESTAMP = "timestamp";
	public static final String SOLR_REFERRER_URI = "referrer_url";
	public static final String SOLR_EXTRACTED_TEXT = "text";
	public static final String SOLR_EXTRACTED_TEXT_LENGTH = "text_length";
	public static final String SOLR_TIKA_METADATA = "tika_metadata";
}