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
import uk.bl.wa.solr.SolrRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Stores documents as SolrXMLDocuments on the file system, either directly or gzipped.
 */
public class FilesystemDocumentConsumer extends BufferedDocumentConsumer {
    private static final Logger log = LoggerFactory.getLogger(FilesystemDocumentConsumer.class);

    private final String filename;
    private final boolean gzip;
    private final Writer out;

    /**
     * Create a FilesystemDocumentConsumer based on the given configuration.
     * @param filename the destination for the documents.
     *                 If gzipOverride is true, {@code .gz} will be appended to the filename.
     * @param gzipOverride if true, the output will be gzipped, else it will be stored directly.
     * @param conf base setup for the DocumentConsumer. Values for maxDocuments, maxBytes and disableCommit will be
     *             taken from here, if present.
     * @param maxDocumentsOverride  if not null, this will override the value from conf "warc.solr.batch_size"
     * @param maxBytesOverride if not null, this will override the value from conf "warc.solr.batch_bytes"
     * @param disableCommitOverride if not null, this will override the value from conf "warc.solr.disablecommit"
     * @return a FilesystemDocumentconsumer, ready for use.
     * @throws IOException if the output file could not be created.
     */
    public FilesystemDocumentConsumer(
            String filename, Config conf, Boolean gzipOverride,
            Integer maxDocumentsOverride, Long maxBytesOverride, Boolean disableCommitOverride) throws IOException {
        super(conf, maxDocumentsOverride, maxBytesOverride, disableCommitOverride);
        gzip = gzipOverride != null && gzipOverride;
        this.filename = filename + (gzip ? ".gz" : "");

        out = gzip ?
                new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                        new FileOutputStream(this.filename))), StandardCharsets.UTF_8) :
                new OutputStreamWriter(new BufferedOutputStream(
                        new FileOutputStream(this.filename)), StandardCharsets.UTF_8);
        out.write("<add>");

        log.info("Constructed " + this);
    }

    @Override
    void performFlush(List<SolrRecord> docs) throws IOException {
        for (SolrRecord record: docs) {
            record.writeXml(out);
        }
    }

    @Override
    void performClose() throws IOException {
        out.write("</add>");
        out.flush();
        out.close();
    }

    @Override
    public void performCommit() {
        // No commit for filesystem
    }

    @Override
    public String toString() {
        return "FilesystemDocumentConsumer{" +
               "filename='" + filename + '\'' +
               "gzip=" + gzip +
               ", inner=" + super.toString() +
               '}';
    }
}
