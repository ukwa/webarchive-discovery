package uk.bl.wap.util.solr;

public interface SolrFields {
	public static final String SOLR_ID = "id";
	public static final String ID_LONG = "id_long";
	public static final String SOLR_URL = "url";
	
	public static final String SOLR_URL_TYPE = "url_type";
	public static final String SOLR_URL_TYPE_SLASHPAGE = "slashpage";
	public static final String SOLR_URL_TYPE_EMBED = "embed";
	
	public static final String SOLR_DOMAIN = "domain";
	public static final String PUBLIC_SUFFIX = "public_suffix";
	public static final String SOLR_DIGEST = "hash";
	public static final String SOLR_TITLE = "title";
	public static final String SOLR_SUBJECT = "subject";
	public static final String SOLR_DESCRIPTION = "description";
	public static final String SOLR_COMMENTS = "comments";
	public static final String SOLR_AUTHOR = "author";
	public static final String SOLR_KEYWORDS = "keywords";
	public static final String SOLR_CATEGORY = "category";
	public static final String SOLR_LINKS = "links";
	public static final String SOLR_LINKS_HOSTS = "hosts";
	public static final String SOLR_LINKS_PUBLIC_SUFFIXES = "public_suffixes";
	
	public static final String SOLR_CONTENT_TYPE = "content_type";
	public static final String CONTENT_ENCODING = "content_encoding";
	public static final String FULL_CONTENT_TYPE = "content_type_full";
	public static final String CONTENT_TYPE_TIKA = "content_type_tika";
	public static final String CONTENT_TYPE_DROID = "content_type_droid";
	public static final String CONTENT_TYPE_SERVED = "content_type_served";
	public static final String SOLR_NORMALISED_CONTENT_TYPE = "content_type_norm";
	public static final String GENERATOR = "generator";
	public static final String PARSE_ERROR = "parse_error";
	
	public static final String CONTENT_LENGTH = "content_length";
	public static final String SOLR_TIMESTAMP = "timestamp";
	public static final String SOLR_REFERRER_URI = "referrer_url";
	public static final String SOLR_EXTRACTED_TEXT = "text";
	public static final String SOLR_EXTRACTED_TEXT_LENGTH = "text_length";
	public static final String SOLR_TIKA_METADATA = "tika_metadata";
	public static final String WAYBACK_DATE = "wayback_date";
	public static final String CRAWL_DATE = "crawl_date";
	public static final String CRAWL_YEAR = "crawl_year";
}