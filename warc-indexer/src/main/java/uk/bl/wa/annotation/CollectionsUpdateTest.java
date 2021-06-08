/**
 * 
 */
package uk.bl.wa.annotation;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

/**
 * 
 * This is a test class to check we understand how updates work.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class CollectionsUpdateTest {

    /**
     * 
     * @see http://192.168.1.204:8990/solr/ldwa/select?q=id%3A20130428112038%2F
     *      nUmQeJ6sh5vz9%2BEBlCHGNA%3D%3D&wt=json&indent=true
     * @id "20130428112038/nUmQeJ6sh5vz9+EBlCHGNA==";
     * 
     * @param args
     * @throws SolrServerException
     * @throws IOException
     */
    public static void main(String[] args) throws SolrServerException,
            IOException {

        String id = "sha1:P566LTRHGUBUAQ7I7FFPNKWESWNXVL3I/fYJsnswpD+25KXMfiuOAlA==";
        String server = "http://localhost:8080/discovery";

        String collection = "Health and Social Care Act 2012 - NHS Reforms";
        String collections = "Health and Social Care Act 2012 - NHS Reforms"
                + "|" + "NHS" + "|" + "Acute Trusts";

        SolrClient ss = new HttpSolrClient.Builder(server).build();

        doQuery(ss, id);

        doUpdate(ss, id, collection, collections);

        doQuery(ss, id);

        doUpdate(ss, id, null, null);

        doQuery(ss, id);

    }

    private static void doUpdate(SolrClient ss, String id, String collection,
            String collections) throws SolrServerException, IOException {

        ss.add(createUpdateDocument(id, collection, collections));

        ss.commit(true, true);

        System.out.println("Updated.");
    }

    private static SolrInputDocument createUpdateDocument(String id,
            String collection, String collections) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);

        Map<String, String> collection_op = new HashMap<String, String>();
        collection_op.put("set", collection);
        doc.addField("collection", collection_op);

        Map<String, String> collections_op = new HashMap<String, String>();
        collections_op.put("set", collections);
        doc.addField("collections", collections_op);

        return doc;
    }

    public static void doQuery(SolrClient ss, String id)
            throws SolrServerException, IOException {
        SolrParams p = new SolrQuery("id:\"" + id + "\"");
        QueryResponse r = ss.query(p);
        System.out.println("GOT collection "
                + r.getResults().get(0).getFieldValue("collection"));
        System.out.println("GOT collections "
                + r.getResults().get(0).getFieldValue("collections"));
        System.out.println("STILL GOT crawl_date "
                + r.getResults().get(0).getFieldValue("crawl_date"));
    }

}
