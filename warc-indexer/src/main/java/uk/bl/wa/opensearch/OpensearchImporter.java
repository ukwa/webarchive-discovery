package uk.bl.wa.opensearch;

/*-
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2023 The webarchive-discovery project contributors
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchImporter {
	private static Logger log = LoggerFactory.getLogger(OpensearchImporter.class);
	static final int MAXRETRIES = 20;
	String indexName;
	OpensearchClient opensearchClient;

	long rowCount = 0;
	boolean initialized = false;
	
	public OpensearchImporter(OpensearchUrl aOpensearchUrl, String aUser, String aPassword) {
		indexName = aOpensearchUrl.getIndexName();
		
		opensearchClient = new OpensearchClient(aOpensearchUrl.getServer(), aUser, aPassword, aOpensearchUrl.getScheme(), aOpensearchUrl.getPort());
	}

	public void importDocuments(List<SolrInputDocument> aDocs) throws Exception {
		if (aDocs == null || aDocs.size() == 0) {
			log.info("no Documents. Nothing to do");
			return;
		}

		BulkRequest bulkRequest = new BulkRequest();

		try {
			for (SolrInputDocument doc : aDocs) {
				XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

				Collection<String> fieldnames = doc.getFieldNames();
				
				String _id = null; 

				for (String fieldname : fieldnames) {
					SolrInputField field = doc.getField(fieldname);
					ArrayList<Object> fieldvalues = new ArrayList<Object>();
					if (field.getValueCount() == 1) {
						if ("id".equals(field.getName())) {
							_id = (String)field.getValue();
							continue;
						}
						else if ("content_type_norm".equals(field.getName())) {
							if (field.getValue() == null || ((String) field.getValue()).length() == 0) {
								// default="other" can not be set in schema
								fieldvalues.add("other");
							} else {
								fieldvalues.add(field.getValue());
							}
						} else {
							fieldvalues.add(field.getValue());
						}
					} else {
						fieldvalues = new ArrayList<Object>(field.getValues());
					}

					builder.field(fieldname, fieldvalues);
				}

				builder.field("index_time", new Date());
				builder.endObject();
				IndexRequest request = new IndexRequest(indexName);
				request.id(_id);
				request.source(builder);
				bulkRequest.add(request);
			}

			BulkResponse bulkResponse;

			for (int i = 1; i <= MAXRETRIES; i++) {
				try {
					bulkResponse = opensearchClient.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
					if (bulkResponse.hasFailures()) {
						Iterator<BulkItemResponse> iterator = bulkResponse.iterator();
						int rowcount = 0;
						while (iterator.hasNext()) {
							rowcount++;
							BulkItemResponse response = iterator.next();
							if (response.isFailed()) {
								log.error("Doc on id " + rowcount + "failed");
							}
						}
						log.error("Bulk insert failure");
						log.error(bulkResponse.buildFailureMessage());
					} else {
						log.info("Bulk insert completed");
					}
					break;
				} catch (IOException e) {
					log.error(e.getMessage());
					log.info("retry " + i);
					if (i == MAXRETRIES) {
						throw new RuntimeException("Max retries reached: " + e.getMessage());
					}
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		} finally {
			if (opensearchClient != null) {
				opensearchClient.close();
			}
		}
	}
}
