/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDX {

	ObjectMapper mapper = new ObjectMapper();

	@JsonProperty
	private String hash;

	@JsonProperty
	private String url;

	@JsonProperty
	private String ts;

	@JsonProperty
	private Map<String, List<String>> properties = new HashMap<String, List<String>>();

	/**
	 * @return the hash
	 */
	public String getHash() {
		return hash;
	}



	/**
	 * @param hash the hash to set
	 */
	public void setHash(String hash) {
		this.hash = hash;
	}



	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}



	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}



	/**
	 * @return the ts
	 */
	public String getTs() {
		return ts;
	}



	/**
	 * @param ts the ts to set
	 */
	public void setTs(String ts) {
		this.ts = ts;
	}



	/**
	 * @return the properties
	 */
	public Map<String, List<String>> getProperties() {
		return properties;
	}

	public String toJSON() {
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toJSON();
	}

	/* --- */

	public static MDX fromJSONString(String json) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, MDX.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param solr
	 * @return
	 */
	public static MDX fromWritabelSolrRecord(SolrRecord solr) {
		MDX m = new MDX();
		m.setHash(solr.getFieldValue(SolrFields.HASH).toString());
		m.setUrl(solr.getFieldValue(SolrFields.SOLR_URL).toString());
		m.setTs(solr.getFieldValue(SolrFields.WAYBACK_DATE).toString());
		// Pass though Solr fields:
		for( String f : solr.getSolrDocument().getFieldNames() ) {
			SolrInputField v = solr.getSolrDocument().get(f);
			Iterator<Object> i = v.getValues().iterator();
			List<String> vals = new ArrayList<String>();
			while (i.hasNext()) {
				vals.add(i.next().toString());
			}
			m.getProperties().put(f, vals);
		}
		
		return m;
	}

}
