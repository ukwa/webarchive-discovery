package uk.bl.wa.elastic;

public class ElasticUrl {
    public static final String PORT_DELIM = ":";
    public static final String SCHEME_DELIM = "://";
    public static final String PATH_DELIM = "/";
    
    public static final String HTTP = "http";
    public static final String HTTPS = "https";

    boolean valid = false;
    String scheme;
    String server;
    int port;
    String indexName;
    
    /**
     * URL format expected is:
     *
     * <code>[scheme]://[host:[port]]/[path]</code>
     *
     * scheme, host, port and path (index) are extracted from the url
     */
    public ElasticUrl(String aUrl) {
    	parse(aUrl);
    }
    
    private void parse(String aUrl) {
    	if (aUrl == null) {
    		return;
    	}
    	
    	if (!aUrl.startsWith(HTTP) && !aUrl.startsWith(HTTPS)) {
    		return;
    	}

    	int schemeidx = aUrl.indexOf(SCHEME_DELIM);
    	
    	if (schemeidx != HTTP.length() && schemeidx != HTTPS.length()) {
    		return;
    	}
    	
    	scheme = aUrl.substring(0,  schemeidx);
    	
    	int afterschemeidx = scheme.length() + SCHEME_DELIM.length();
    	int portbeginidx = aUrl.indexOf(PORT_DELIM, afterschemeidx);
    	
    	int pathidx;
    	
    	if (portbeginidx == -1) {
    		port = 80;
    		pathidx = aUrl.indexOf(PATH_DELIM, afterschemeidx);
    		
    		if (pathidx == -1 || pathidx - afterschemeidx == 1) {
    			return;
    		}
    		
    		server = aUrl.substring(afterschemeidx, pathidx);
    	}
    	else {
        	int portendidx = aUrl.indexOf(PATH_DELIM, portbeginidx);
        	if (portendidx == -1) {
        		return;
        	}
        	
        	try {
            	port = Integer.parseInt(aUrl.substring(portbeginidx + 1, portendidx));
        	}
        	catch(NumberFormatException e) {
        		return;
        	}
        	
        	server = aUrl.substring(afterschemeidx, portbeginidx);
        	
    		pathidx = aUrl.indexOf(PATH_DELIM, portendidx);

    		if (pathidx == -1 || pathidx - portendidx == 1) {
    			return;
    		}
    	}

    	if (aUrl.length() == pathidx + 1) {
    		return;
    	}
    	
		indexName = aUrl.substring(pathidx + 1);
		
		pathidx = indexName.indexOf(PATH_DELIM);
		if (pathidx != -1) {
			indexName = indexName.substring(0, pathidx);
		}
		
		valid = true;
    }

    public boolean isValid() {
    	return valid;
    }

    public String getScheme() {
    	return scheme;
    }
    
    public String getServer() {
    	return server;
    }
    
    public int getPort() {
    	return port;
    }
    
    public String getIndexName() {
    	return indexName;
    }
}
