/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package uk.bl.wa.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;

/**
 *
 */
public abstract class SolrDocumentConsumer extends BufferedDocumentConsumer {
    private static final Logger log = LoggerFactory.getLogger(SolrDocumentConsumer.class);

    private final String solrURL;

    /**
     * Create a SolrDocumentConsumer based on the given configuration.
     * @param solrURL the URL to a Solr installation, e.g. {@code https://ourserver.internal:8989/solr/outarchive}
     * @param conf base setup for the DocumentConsumer. Values for maxDocuments, maxBytes and disableCommit will be
     *             taken from here, if present.
     * @param maxDocumentsOverride  if not null, this will override the value from conf "warc.solr.batch_size"
     * @param maxBytesOverride if not null, this will override the value from conf "warc.solr.batch_bytes"
     * @param disableCommitOverride if not null, this will override the value from conf "warc.solr.disablecommit"
     * @return a SolrDocumentconsumer, ready for use.
     */
    public SolrDocumentConsumer(String solrURL, Config conf,
                                Integer maxDocumentsOverride, Long maxBytesOverride, Boolean disableCommitOverride) {
        super(conf, maxDocumentsOverride, maxBytesOverride, disableCommitOverride);
        this.solrURL = solrURL;
    }


}
