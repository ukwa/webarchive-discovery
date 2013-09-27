/**
 * 
 */
package uk.bl.wa.shine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;

import play.Logger;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Query {

	public String query;
	
	public Map<String,List<String>> filters;
	
	public QueryResponse res;
	
	public void parseParams( Map<String,List<String>> params ) {
		filters = new HashMap<String, List<String>>();
		for( String param : params.keySet() ) {
			if( param.startsWith("facet.in.")) {
			    filters.put(param.replace("facet.in.", ""), params.get(param));
			} else if( param.startsWith("facet.out.")) {
			    filters.put("-"+param.replace("facet.out.", ""), params.get(param));
			}
		}
	}
	
	public String getCheckedInString(String facet_name, String value ) {
		for( String fc : filters.keySet() ) {
			if( fc.equals(facet_name) && filters.get(fc).contains("\""+value+"\"")) return "checked=\"\"";
		}
		return "";
	}
	
	public String getCheckedOutString(String facet_name, String value ) {
		return this.getCheckedInString("-"+facet_name, value);
	}
	
	public String getParamsPlusFilter(String facet_name, String facet_value) {
		String qp = "";
//		Logger.info("---- ----");
		for( String key : res.getFacetQuery().keySet() ) {
//			Logger.info(key+">"+res.getFacetQuery().get(key));
		}
		for( FacetField fc: res.getLimitingFacets() ) {
//			Logger.info("LF: "+fc);
		}
		for( FacetField fc : this.res.getFacetFields() ) {
//			Logger.info("FF: "+fc);
			if( fc.getName().equals(facet_name) ) {
				
			}
		}
		return qp;
	}
}
