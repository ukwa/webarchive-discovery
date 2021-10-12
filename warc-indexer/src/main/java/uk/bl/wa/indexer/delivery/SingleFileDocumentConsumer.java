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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.solr.SolrRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Stores documents as SolrXMLDocuments on the file system as 1 file/WARC, optionally gzipped.
 */
public class SingleFileDocumentConsumer extends BufferedDocumentConsumer {
    private static final Logger log = LoggerFactory.getLogger(SingleFileDocumentConsumer.class);

    private final boolean gzip;
    private final String outputFolder;

    private String filename = null;
    private Writer out = null;

    /**
     * Create a FilesystemDocumentConsumer based on the given configuration.
     * @param gzipOverride if true, the output will be gzipped, else it will be stored directly.
     * @param conf base setup for the DocumentConsumer. Values for maxDocuments, maxBytes and disableCommit will be
     *             taken from here, if present.
     * @param maxDocumentsOverride  if not null, this will override the value from conf "warc.solr.batch_size"
     * @param maxBytesOverride if not null, this will override the value from conf "warc.solr.batch_bytes"
     * @return a FilesystemDocumentconsumer, ready for use.
     * @throws IOException if the output file could not be created.
     */
    public SingleFileDocumentConsumer(
            String outputFolder, Config conf, Boolean gzipOverride,
            Integer maxDocumentsOverride, Long maxBytesOverride) throws IOException {
        super(conf, maxDocumentsOverride, maxBytesOverride, true); // Commit is implicit
        gzip = gzipOverride != null && gzipOverride;
        this.outputFolder = outputFolder + (outputFolder.endsWith("/") || outputFolder.endsWith("\\") ? "" : "/");

        Path outPath = new File(this.outputFolder).toPath();
        if (!Files.exists(outPath)) {
            try {
                Files.createDirectories(outPath);
            } catch (Exception e) {
                throw new IOException("Unable to create output folder '" + outPath + "'");
            }
        }
        log.info("Constructed " + this);
    }

    @Override
    void performFlush(List<SolrRecord> docs) throws IOException {
        if (out == null) {
            throw new IllegalStateException("Flush called but no output file is defined");
        }
        for (SolrRecord record: docs) {
            record.writeXml(out);
        }
        // "</add>" is not appended, as it only makes sense on close, so this is not strictly correct behaviour
        // as defined in DocumentConsumer#flush. Not much to do about that.
        out.flush();
    }

    @Override
    void performClose() throws IOException {
        endWARC();
    }

    @Override
    public void performCommit() {
        // No commit for filesystem
    }

    @Override
    public void startWARC(String warcfile) throws IOException {
        File inFile = new File(warcfile);
        filename = outputFolder + inFile.getName() + ".xml" + (gzip ? ".gz" : "");
        out = gzip ?
                new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(
                        new FileOutputStream(filename))), StandardCharsets.UTF_8) :
                new OutputStreamWriter(new BufferedOutputStream(
                        new FileOutputStream(filename)), StandardCharsets.UTF_8);
        out.write("<add>");
    }

    @Override
    public void endWARC() throws IOException {
        if (out == null) {
            return;
        }
        flush(); // Ensure all buffered documents are written
        out.write("</add>");
        out.flush();
        out.close();

        out = null;
        filename = null;
    }

    @Override
    public String toString() {
        return "SingleFileDocumentConsumer{" +
               "outputFolder='" + filename + '\'' +
               ", gzip=" + gzip +
               ", inner=" + super.toString() +
               '}';
    }
}
