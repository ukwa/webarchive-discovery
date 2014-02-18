/**
 * 
 */
package uk.bl.wa.indexer;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.archive.format.gzip.GZIPDecoder;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.util.ArchiveUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.util.solr.SolrRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WARCIndexerEmbeddedSolrTest {

	private String testWarc = "src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz";
	//private String testWarc = "src/test/resources/wikipedia-mona-lisa/flashfrozen.warc.gz";
	//private String testWarc = "src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc";
	//private String testWarc = "src/test/resources/variations.warc.gz";
	//private String testWarc = "src/test/resources/TEST.arc.gz";
	//String testWarc = "src/test/resources/IAH-20080430204825-00000-blackbook-truncated.warc.gz";
	String testArc = "src/test/resources/IAH-20080430204825-00000-blackbook-truncated.arc.gz";
	
	private EmbeddedSolrServer server;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
		// Note that the following property could be set through JVM level arguments too
		  System.setProperty("solr.solr.home", "src/main/solr/solr");
		  System.setProperty("solr.data.dir", "target/solr-test-home");
		  CoreContainer coreContainer = new CoreContainer();
		  coreContainer.load();
		  server = new EmbeddedSolrServer(coreContainer, "");
		  // Remove any items from previous executions:
		  server.deleteByQuery("*:*");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		server.shutdown();
	}

	/**
	 * 
	 * @throws SolrServerException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 * @throws ResourceNotAvailableException 
	 */
	@Test
	public void testEmbeddedServer() throws SolrServerException, IOException, NoSuchAlgorithmException, TransformerFactoryConfigurationError, TransformerException {
		// Fire up a SOLR:
		String url = "http://www.lafromagerie.co.uk/cheese-room/?milk=buffalo&amp%3Bamp%3Bamp%3Bamp%3Borigin=wales&amp%3Bamp%3Bamp%3Borigin=switzerland&amp%3Bamp%3Borigin=germany&amp%3Bstyle=semi-hard&style=blue";
		SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "1");
        document.addField("name", "my name");
        document.addField( "url", url );

        System.out.println("Adding document: "+document);
        server.add(document);
        server.commit();
        
        System.out.println("Querying for document...");
        SolrParams params = new SolrQuery("name:name");
        QueryResponse response = server.query(params);
        assertEquals(1L, response.getResults().getNumFound());
        assertEquals("1", response.getResults().get(0).get("id"));

        // Check that URLs are encoding correctly.
        assertEquals( url, document.getFieldValue( "url" ) );
		assertEquals( url, response.getResults().get( 0 ).get( "url" ) );
        
        //  Now generate some Solr documents from WARCs:
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		// Spin up WARCIndexer
		WARCIndexer windex = new WARCIndexer();
		ArchiveReader reader = ArchiveReaderFactory.get( testWarc );
		System.out.println("ArchiveUtils.isGZipped: "+ArchiveUtils.isGzipped( new FileInputStream(testWarc)));
		new GZIPDecoder().parseHeader( new FileInputStream(testWarc) );
		Iterator<ArchiveRecord> ir = reader.iterator();
		int total = 0;
		while( ir.hasNext() ) {
			ArchiveRecord rec = ir.next();
			total++;
			SolrRecord doc = windex.extract( rec.getHeader().getUrl(), rec );
			if( doc != null ) {
				//System.out.println(doc.toXml());
				//break;
				docs.add(doc.doc);
			} else {
				//System.out.println("Got a NULL document for: "+rec.getHeader().getUrl());
			}
			//System.out.println(" ---- ---- ");
		}
		System.out.println("Added "+docs.size()+" docs out of "+total+" .");
		// Check the read worked:
        assertEquals(39L, docs.size());

        server.add(docs);
        server.commit();

        // Now query:
        params = new SolrQuery("content_type:image*");
        //params = new SolrQuery("generator:*");
        response = server.query(params);
        for( SolrDocument result : response.getResults() ) {
        	for( String f : result.getFieldNames() ) {
        		System.out.println(f + " -> " + result.get(f));
        	}
        }
        assertEquals(21L, response.getResults().getNumFound());

	}

}
