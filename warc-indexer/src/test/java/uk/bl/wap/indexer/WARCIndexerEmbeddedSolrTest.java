/**
 * 
 */
package uk.bl.wap.indexer;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexerEmbeddedSolrTest {

	private EmbeddedSolrServer server;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
		// Note that the following property could be set through JVM level arguments too
		  System.setProperty("solr.solr.home", "target/test-classes/solr");
		  CoreContainer.Initializer initializer = new CoreContainer.Initializer();
		  CoreContainer coreContainer = initializer.initialize();
		  server = new EmbeddedSolrServer(coreContainer, "");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		server.shutdown();
	}

	@Test
	public void test() throws SolrServerException, IOException {
		SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "1");
        document.addField("name", "my name");

        server.add(document);
        server.commit();

        SolrParams params = new SolrQuery("name");
        QueryResponse response = server.query(params);
        assertEquals(1L, response.getResults().getNumFound());
        assertEquals("1", response.getResults().get(0).get("id"));
        
        
		fail("Not yet implemented");
	}

}
