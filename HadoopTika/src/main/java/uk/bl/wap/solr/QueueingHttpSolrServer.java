package uk.bl.wap.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class QueueingHttpSolrServer extends CommonsHttpSolrServer {
	private static final long serialVersionUID = 2827955704705820516L;
	private int queueSize = 50;
	Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	public QueueingHttpSolrServer( String solrServerUrl, int queueSize ) throws MalformedURLException {
		super( solrServerUrl );
		this.queueSize = queueSize;
	}

	@Override
	public UpdateResponse add( SolrInputDocument doc ) throws SolrServerException, IOException {
		this.docs.add( doc );
		return checkQueue();
	}

	@Override
	public UpdateResponse add( Collection<SolrInputDocument> docs ) throws SolrServerException, IOException {
		this.docs.addAll( docs );
		return checkQueue();
	}

	@Override
	public UpdateResponse add( Iterator<SolrInputDocument> docIterator ) throws SolrServerException, IOException {
		while( docIterator.hasNext() ) {
			this.docs.add( docIterator.next() );
		}
		return checkQueue();
	}

	public UpdateResponse flush() throws SolrServerException, IOException {
		UpdateResponse response;
		if( this.docs.size() > 0 ) {
			response = super.add( docs );
			this.docs.clear();
		} else {
			response = new UpdateResponse();
		}
		return response;
	}

	private UpdateResponse checkQueue() throws SolrServerException, IOException {
		if( this.docs.size() >= this.queueSize ) {
			return flush();
		} else {
			return new UpdateResponse();
		}
	}
}
