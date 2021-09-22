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

/*-
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

import uk.bl.wa.solr.SolrRecord;

import java.io.Closeable;
import java.io.IOException;

/**
 * Receives {@link uk.bl.wa.solr.SolrRecord}s, buffers them and sends them in batches to an implementation specific
 * destination such as the file system, Solr or Elasticsearch.
 */
public interface DocumentConsumer extends Closeable {
    /**
     * Add a record to the consumer. This might cause af flush of buffered documents.
     * @param solrRecord the record to add.
     * @throws IOException if a flush was attempted and could not be performed.
     */
    void add(SolrRecord solrRecord) throws IOException;

    /**
     * Perform an explicit flush of buffered documents. After this, all buffered documents should have been
     * fully consumed, i.e. send to Solr/Elasticsearch or written to the file system.
     * @throws IOException if the flush could not be completed.
     */
    void flush() throws IOException;

    /**
     * Trigger a commit at the receiving end of the document pipeline.
     * The commit is NOT called automatically on {@link #close()}.
     * @throws IOException if the commit could not be completed.
     */
    void commit() throws IOException;

    /**
     * Ensures that all buffered content is flushed and closes all resources. After close the state of the consumer
     * is undefined and the consumer should not be used.
     * @throws IOException if the close failed.
     */
    @Override
    default void close() throws IOException {
        flush();
    }

    /**
     * Signals that a new WARC is about to be processed.
     * @param warcfile the warc file that will be processed.
     * @throws IOException if the warc start signal caused problems.
     */
    default void startWARC(String warcfile) throws IOException {
        // Do nothing per default
    }

    /**
     * Signals that the processing of a WARC file has finished.
     * DocumentConsumer implementations are responsible for calling {@link #flush()} if needed.
     * @throws IOException if the warc end signal caused problems.
     */
    default void endWARC() throws IOException {
        // Do nothing per default
    }
}
