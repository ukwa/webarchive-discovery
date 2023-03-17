package uk.bl.wa.analyser;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.io.ArchiveRecordHeader;

/*
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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import junit.framework.TestCase;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;

public class WARCPayloadAnalysersTest extends TestCase {
    private static Logger log = LoggerFactory.getLogger(WARCPayloadAnalysersTest.class);

    public void testProcessContentType() {
        WARCPayloadAnalysers ana = getWARCPayloadAnalysers();

        // For ISSUE-289: https://github.com/ukwa/webarchive-discovery/issues/289
        // Set up a test with the Header/served content type, but the identified content type is 'application/octet-stream'
        ArchiveRecordHeader header = new FakeHeader("whatever/localrun-job87-20150219-133227.warc", "video/mp4");
        SolrRecord solr = SolrRecordFactory.createFactory(null).createRecord();
        solr.setField(SolrFields.SOLR_CONTENT_TYPE, "application/octet-stream");

        // Determine the full and normalised content types from there:
        ana.processContentType(solr, header, 1000, false);

        // If other format identification has failed, the served type should win:
        assertEquals("The final content type should fall back on the served content type", "video/mp4", solr.getFieldValue(SolrFields.FULL_CONTENT_TYPE));
        assertEquals("The final normalized content type should fall back on the served content type", "video", solr.getFieldValue(SolrFields.SOLR_NORMALISED_CONTENT_TYPE));
    }
    private WARCPayloadAnalysers getWARCPayloadAnalysers() {
        Config conf = ConfigFactory.parseURL(
                Thread.currentThread().getContextClassLoader().getResource("reference.conf"));
        return new WARCPayloadAnalysers(conf);
    }

    private class FakeHeader implements ArchiveRecordHeader {
        private final String arcPath;
        private final String mimeType;
        public FakeHeader(String arcPath, String mimeType) {
            this.arcPath = arcPath;
            this.mimeType = mimeType;
        }

        @Override
        public String getDate() {
            return null;
        }

        @Override
        public long getLength() {
            return 0;
        }

        @Override
        public long getContentLength() {
            return 1000;
        }

        @Override
        public String getUrl() {
            return null;
        }

        @Override
        public String getMimetype() {
            return this.mimeType;
        }

        @Override
        public String getVersion() {
            return null;
        }

        @Override
        public long getOffset() {
            return 0;
        }

        @Override
        public Object getHeaderValue(String key) {
            return null;
        }

        @Override
        public Set<String> getHeaderFieldKeys() {
            return new HashSet<String>();
        }

        @Override
        public Map<String, Object> getHeaderFields() {
            return null;
        }

        @Override
        public String getReaderIdentifier() {
            return arcPath;
        }

        @Override
        public String getRecordIdentifier() {
            return null;
        }

        @Override
        public String getDigest() {
            return null;
        }

        @Override
        public int getContentBegin() {
            return 0;
        }
    }
}
