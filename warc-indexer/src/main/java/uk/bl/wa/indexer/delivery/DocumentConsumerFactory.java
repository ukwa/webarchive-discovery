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
package uk.bl.wa.indexer.delivery;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Documentconsumer is responsible for receiving {@link uk.bl.wa.solr.SolrRecord}s and passing them on to a
 * receiving system, such as Solr, Elasticsearch or the file system.
 */
public class DocumentConsumerFactory {
    private static final Logger log = LoggerFactory.getLogger(DocumentConsumerFactory.class);

    public static final String BATCH_DOCUMENTS_KEY = "warc.solr.batch_size";
    public static final String BATCH_BYTES_KEY = "warc.solr.batch_bytes";
    public static final String DISABLE_COMMIT_KEY = "warc.solr.disablecommit";

    public static final int BATCH_DOCUMENTS_DEFAULT = 1;
    public static final int BATCH_BYTES_DEFAULT = 20_000_000; // ~20MB
    public static final boolean DISABLE_COMMIT_DEFAULT = false;

    /**
     * Create a DocumentConsumer based on the given configuration.
     * @param conf base setup for the DocumentConsumer.
     * @param maxDocumentsOverride  if not null, this will override the value from conf "warc.solr.batch_size"
     * @param maxBytesOverride if not null, this will override the value from conf "warc.solr.batch_bytes"
     * @param disableCommitOverride if not null, this will override the value from conf "warc.solr.disablecommit"
     * @return
     */
    public static DocumentConsumer createConsumer(
            Config conf, String outputDir, boolean outputGZIP,
            Integer maxDocumentsOverride, Long maxBytesOverride, Boolean disableCommitOverride) {
        int maxDocuments = maxDocumentsOverride != null ? maxDocumentsOverride :
                conf.hasPath(BATCH_DOCUMENTS_KEY) ? conf.getInt(BATCH_DOCUMENTS_KEY) :
                        BATCH_DOCUMENTS_DEFAULT;
        long maxBytes = maxBytesOverride != null ? maxBytesOverride :
                conf.hasPath(BATCH_BYTES_KEY) ? conf.getLong(BATCH_BYTES_KEY) :
                        BATCH_BYTES_DEFAULT;
        boolean disableCommit = disableCommitOverride != null ? disableCommitOverride :
                conf.hasPath(DISABLE_COMMIT_KEY) ? conf.getBoolean(DISABLE_COMMIT_KEY) :
                        DISABLE_COMMIT_DEFAULT;
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
