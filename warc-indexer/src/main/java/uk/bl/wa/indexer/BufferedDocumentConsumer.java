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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.solr.SolrRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base implementation of a buffered DocumentConsumer. Takes care of tracking and buffering documents,
 * calling flush when needed.
 */
public abstract class BufferedDocumentConsumer implements DocumentConsumer {
    private static final Logger log = LoggerFactory.getLogger(BufferedDocumentConsumer.class);

    public static final String BATCH_DOCUMENTS_KEY = "warc.solr.batch_size";
    public static final String BATCH_BYTES_KEY = "warc.solr.batch_bytes";
    public static final String DISABLE_COMMIT_KEY = "warc.solr.disablecommit";

    public static final int BATCH_DOCUMENTS_DEFAULT = 1;
    public static final int BATCH_BYTES_DEFAULT = 20_000_000; // ~20MB
    public static final boolean DISABLE_COMMIT_DEFAULT = false;

    private final int maxDocuments;
    private final long maxBytes;
    private final boolean commitOnClose;

    private final List<SolrRecord> docs;
    private final AtomicLong totalBytes = new AtomicLong(0);
    boolean isClosed = false;

    /**
     * Create a DocumentConsumer based on the given configuration.
     * @param conf base setup for the DocumentConsumer. Values for maxDocuments, maxBytes and disableCommit will be
     *             taken from here, if present.
     * @param maxDocumentsOverride  if not null, this will override the value from conf "warc.solr.batch_size"
     * @param maxBytesOverride if not null, this will override the value from conf "warc.solr.batch_bytes"
     * @param disableCommitOverride if not null, this will override the value from conf "warc.solr.disablecommit"
     * @return
     */
    public BufferedDocumentConsumer(
            Config conf, Integer maxDocumentsOverride, Long maxBytesOverride, Boolean disableCommitOverride) {
        this.maxDocuments = maxDocumentsOverride != null ? maxDocumentsOverride :
                conf != null && conf.hasPath(BATCH_DOCUMENTS_KEY) ? conf.getInt(BATCH_DOCUMENTS_KEY) :
                        BATCH_DOCUMENTS_DEFAULT;
        this.maxBytes = maxBytesOverride != null ? maxBytesOverride :
                conf != null && conf.hasPath(BATCH_BYTES_KEY) ? conf.getLong(BATCH_BYTES_KEY) :
                        BATCH_BYTES_DEFAULT;
        this.commitOnClose = !(disableCommitOverride != null ? disableCommitOverride :
                conf != null && conf.hasPath(DISABLE_COMMIT_KEY) ? conf.getBoolean(DISABLE_COMMIT_KEY) :
                        DISABLE_COMMIT_DEFAULT);
        docs = new ArrayList<>(maxDocuments == -1 ? 1000 : maxDocuments);
    }

    @Override
    public synchronized void add(SolrRecord solrRecord) throws IOException {
        docs.add(solrRecord);
        if (maxBytes != -1 && totalBytes.addAndGet(solrRecord.getApproximateSize()) >= maxBytes) {
            log.debug("Calling flush as totalBytes={} > maxBytes={}", totalBytes, maxBytes);
            flush();
        } else if (maxDocuments != -1 && docs.size() >= maxDocuments){
            log.debug("Calling flush as #documents == maxDocuments == {}", maxDocuments);
            flush();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        if (docs.isEmpty()) {
            log.debug("Flush called with no documents buffered");
            return;
        }
        log.debug("Flushing {} documents", docs);
        flush(Collections.unmodifiableList(docs));
        docs.clear();
        totalBytes.set(0);
    }

    @Override
    public void close() throws IOException {
        log.debug("Flushing, optionally committing and closing");
        flush();
        if (commitOnClose) {
            log.debug("Triggering commit");
            commit();
        }
        performClose();
        isClosed = true;
    }

    /**
     * Flush the given documents. This will be called automatically by {@link #add(SolrRecord)}, {@link #flush()} and
     * {@link #close()}.
     * @param docs the documents to flush.
     * @throws IOException if the flush failed.
     */
    abstract void flush(List<SolrRecord> docs) throws IOException;

    /**
     * Perform any close procedure needed by the consumer. This is called automatically by {@link #close()}.
     * @throws IOException if the close procedure failed.
     */
    abstract void performClose() throws IOException;

}
