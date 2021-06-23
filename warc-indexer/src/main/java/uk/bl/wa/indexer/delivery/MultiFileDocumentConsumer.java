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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Stores documents as SolrXMLDocuments on the file system as 1 file/WARC-record.
 */
public class MultiFileDocumentConsumer implements DocumentConsumer {
    private static final Logger log = LoggerFactory.getLogger(MultiFileDocumentConsumer.class);

    private final boolean gzip;
    private final String rootFolder;

    private String currentFolder = null;
    private final AtomicLong fileCounter = new AtomicLong(0);

    /**
     * Create a MultiFileDocumentConsumer based on the given configuration.
     * @param gzipOverride if true, the outputs will be gzipped, else they will be stored directly.
     * @param conf base setup for the DocumentConsumer. Values for maxDocuments, maxBytes and disableCommit will be
     *             taken from here, if present.
     * @return a DocumentConsumer, ready for use.
     * @throws IOException if the output file could not be created.
     */
    public MultiFileDocumentConsumer(
            String outputFolder, Config conf, Boolean gzipOverride) throws IOException {
        this.gzip = gzipOverride != null && gzipOverride;
        this.rootFolder = outputFolder + (outputFolder.endsWith("/") ? "" : "/");

        createFolder(rootFolder);
        log.info("Constructed " + this);
    }

    @Override
    public void add(SolrRecord solrRecord) throws IOException {
        if (currentFolder == null) {
            throw new IllegalStateException(
                    "Error: currentFolder==null when adding SolrRecord: startWARC was probably not called");
        }

        File filename = new File(String.format(Locale.ROOT, "%s/FILE_%5d.xml%s",
                                               currentFolder, fileCounter.incrementAndGet(), gzip ? ".gz" : ""));
        Writer out = gzip ?
                new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                        new FileOutputStream(filename))), StandardCharsets.UTF_8) :
                new OutputStreamWriter(new BufferedOutputStream(
                        new FileOutputStream(filename)), StandardCharsets.UTF_8);
        solrRecord.writeXml(out);
        out.flush();
        out.close();
        log.debug("Wrote SolrXMLDocument to '{}'", filename);
    }

    @Override
    public void flush() throws IOException {
        // No action: All additions are immediately flushed
    }

    @Override
    public void commit() throws IOException {
        // No action: All additions are immediately flushed/committed
    }

    @Override
    public void startWARC(String warcfile) throws IOException {
        File inFile = new File(warcfile);
        currentFolder = createFolder(rootFolder + inFile.getName());
    }

    @Override
    public void endWARC() throws IOException {
        currentFolder = null;
        fileCounter.set(0);
    }

    private String createFolder(String folder) throws IOException {
        Path outPath = new File(folder).toPath();
        if (!Files.exists(outPath)) {
            try {
                Files.createDirectories(outPath);
            } catch (Exception e) {
                throw new IOException("Unable to create folder '" + outPath + "'");
            }
        }
        return folder;
    }

    @Override
    public String toString() {
        return "MultiFileDocumentConsumer{" +
               "rootFolder='" + rootFolder + '\'' +
               "gzip=" + gzip +
               '}';
    }
}
