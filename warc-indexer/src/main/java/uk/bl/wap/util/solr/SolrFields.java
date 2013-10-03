package uk.bl.wap.util.solr;

public interface SolrFields {
	public static final String SOLR_ID = "id";
	public static final String ID_LONG = "id_long";
	public static final String SOLR_URL = "url";
	
	public static final String SOLR_URL_TYPE = "url_type";
	public static final String SOLR_URL_TYPE_SLASHPAGE = "slashpage";
	public static final String SOLR_URL_TYPE_EMBED = "embed";
	
	public static final String SOLR_HOST = "domain";
	public static final String PUBLIC_SUFFIX = "public_suffix";
	public static final String DOMAIN = "private_suffix";
	public static final String SOLR_DIGEST = "hash";
	public static final String SOLR_TITLE = "title";
	public static final String SOLR_SUBJECT = "subject";
	public static final String SOLR_DESCRIPTION = "description";
	public static final String SOLR_COMMENTS = "comments";
	public static final String SOLR_AUTHOR = "author";
	public static final String SOLR_KEYWORDS = "keywords";
	public static final String SOLR_CATEGORY = "category";
	public static final String SOLR_LINKS = "links";
	public static final String SOLR_LINKS_HOSTS = "links_hosts";
	public static final String SOLR_LINKS_PUBLIC_SUFFIXES = "links_public_suffixes";
	public static final String SOLR_LINKS_PRIVATE_SUFFIXES = "links_private_suffixes";
	
	public static final String SOLR_CONTENT_TYPE = "content_type";
	public static final String CONTENT_ENCODING = "content_encoding";
	public static final String CONTENT_LANGUAGE = "content_language";
	public static final String FULL_CONTENT_TYPE = "content_type_full";
	public static final String CONTENT_TYPE_TIKA = "content_type_tika";
	public static final String CONTENT_TYPE_DROID = "content_type_droid";
	public static final String CONTENT_TYPE_SERVED = "content_type_served";
	public static final String CONTENT_TYPE_EXT = "content_type_ext";
	public static final String SOLR_NORMALISED_CONTENT_TYPE = "content_type_norm";
	public static final String CONTENT_FFB = "content_ffb";
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
	public static final String PUBLICATION_DATE = "publication_date";
	public static final String PUBLICATION_YEAR = "publication_year";
	public static final String LAST_MODIFIED = "last_modified";
	public static final String LAST_MODIFIED_YEAR = "last_modified_year";
	
	public static final String POSTCODE = "postcode";
	public static final String POSTCODE_DISTRICT = "postcode_district";
	public static final String LOCATIONS = "locations";
	
	public static final String SENTIMENT = "sentiment";
	public static final String[] SENTIMENTS = new String[] {"Very Negative", "Negative", "Mildly Negative" ,"Neutral", "Mildly Positive", "Positive", "Very Positive"};
	public static final String SENTIMENT_SCORE = "sentiment_score";
	
	public static final String LICENSE_URL = "license_url";
	
	public static final String SSDEEP_PREFIX = "ssdeep_hash_bs_";
	public static final String SSDEEP_NGRAM_PREFIX = "ssdeep_hash_ngram_bs_";
	
	public static final String ELEMENTS_USED = "elements_used";
}