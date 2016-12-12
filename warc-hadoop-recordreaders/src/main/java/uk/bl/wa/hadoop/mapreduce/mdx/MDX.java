/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.mdx;

import org.apache.commons.httpclient.URIException;
import org.archive.url.SURTTokenizer;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This defines an MDX record, containing minimal explicit metadata and a
 * general hash-map of arrays for other data.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDX extends JSONObject {

    // Main fields, from CDXJ
    public static final String URL_KEY = "url-key"; // The URL key, often
                                                    // canonicalised, SURTed
                                                    // etc.
    public static final String TIMESTAMP = "timestamp"; // Should be
                                                        // YYYYMMDDhhmmss
                                                        // format.
    public static final String URL = "url";
    public static final String DIGEST = "digest";
    public static final String LENGTH = "length";
    public static final String OFFSET = "offset";
    public static final String FILENAME = "filename";

    // Additional metadata fields:
    public static final String RECORD_TYPE = "record-type";
    public static final String DATE = "date"; // Crawl date in ISO format

    public MDX(String jsonString) throws JSONException {
        super(jsonString);
    }

    public MDX() {
        super();
    }

    /**
     * @return the hash
     * @throws JSONException
     */
    public String getHash() {
        try {
            return this.getString(DIGEST);
        } catch (JSONException e) {
            return null;
        }
    }

	/**
     * @param hash
     *            the hash to set
     * @throws JSONException
     */
    public void setHash(String hash) throws JSONException {
        this.put(DIGEST, hash);
    }

    /**
     * @return the url
     * @throws JSONException
     */
    public String getUrl() {
        try {
            return this.getString(URL);
        } catch (JSONException e) {
            return null;
        }
    }

    /**
     * 
     * @return
     * @throws JSONException
     */
	@JsonIgnore
    public String getUrlAsSURT() throws JSONException {
        String url = this.getString(URL);
		try {
			return SURTTokenizer.exactKey(url);
		} catch (URIException e) {
			// Fall back on normal URI:
			return url;
		}
	}

    /**
     * @param url
     *            the url to set
     * @throws JSONException
     */
    public void setUrl(String url) throws JSONException {
        this.put(URL, url);
    }

    /**
     * @return the ts
     * @throws JSONException
     */
    public String getTs() {
        try {
            return this.getString(TIMESTAMP);
        } catch (JSONException e) {
            return null;
        }
    }

    /**
     * @param ts
     *            the ts to set
     * @throws JSONException
     */
    public void setTs(String ts) throws JSONException {
        this.put(TIMESTAMP, ts);
    }

    /**
     * @return the record_type
     * @throws JSONException
     */
    public String getRecordType() {
        try {
            return this.getString(RECORD_TYPE);
        } catch (JSONException e) {
            return null;
        }
    }

    /**
     * @param record_type
     *            the record_type to set
     * @throws JSONException
     */
    public void setRecordType(String recordType) throws JSONException {
        this.put(RECORD_TYPE, recordType);
    }

}
