package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2015 State and University Library, Denmark
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
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.archive.io.ArchiveRecordHeader;
import uk.bl.wa.solr.SolrRecord;

import java.util.Map;
import java.util.Set;

public class WARCPayloadAnalysersTest extends TestCase {
    private static Log log = LogFactory.getLog(WARCPayloadAnalysersTest.class);

    public void testConfig() {
        ARCNameAnalyser ana = getAnalyser();
        assertEquals("The expected number of rules should be created",
                     1, ana.getRules().size());
        assertEquals("The number of templates for the first rule should be correct",
                     2, ana.getRules().get(0).templates.size());
    }

    public void testSampleRule() {
        ARCNameAnalyser ana = getAnalyser();

        ArchiveRecordHeader header = new FakeHeader("whatever/localrun-job87-20150219-133227.warc");
        SolrRecord solr = new SolrRecord();
        ana.analyse(header, null, solr);
        assertEquals("The solr documents should have the right content for field harvest_job",
                     "job87", (solr.getFieldValue("harvest_job").toString()));
        assertEquals("The solr documents should have the right content for field harvest_year",
                     "2015", solr.getFieldValue("harvest_year").toString());
    }

    public void testSBRules() { // Local rules used at Statsbiblioteket
        ARCNameAnalyser ana = getAnalyser();
        for (String test[]: new String[][]{
                {
                        "arc_orig:sb, arc_harvesttime:2008-02-21T00:35:33.000Z, arc_job:25666, arc_harvest:33, "
                        + "arc_name:25666-33-20080221003533-00046-sb-prod-har-004.arc, "
                        + "arc_full:25666-33-20080221003533-00046-sb-prod-har-004.arc",
                        "25666-33-20080221003533-00046-sb-prod-har-004.arc"
                },
                {
                        "arc_orig:sb, arc_harvesttime:2007-04-18T02:46:37.000Z, arc_job:15626, arc_harvest:38, "
                        + "arc_name:15626-38-20070418024637-00385-sb-prod-har-001.statsbiblioteket.dk.arc, "
                        + "arc_full:/netarkiv/0101/filedir/15626-38-20070418024637-00385-sb-prod-har-001.statsbiblioteket.dk.arc",
                        "/netarkiv/0101/filedir/15626-38-20070418024637-00385-sb-prod-har-001.statsbiblioteket.dk.arc"
                },
                {
                        "arc_orig:kb, arc_harvesttime:2007-04-18T16:37:59.000Z, arc_job:15638, arc_harvest:38, "
                        + "arc_name:15638-38-20070418163759-00235-kb-prod-har-002.kb.dk.arc, "
                        + "arc_full:somepath/15638-38-20070418163759-00235-kb-prod-har-002.kb.dk.arc",
                        "somepath/15638-38-20070418163759-00235-kb-prod-har-002.kb.dk.arc"
                },
                {
                        "arc_orig:kb, arc_harvesttime:2013-11-11T17:55:47.000Z, arc_job:193305, arc_harvest:197, "
                        + "arc_name:193305-197-20131111175547-00001-kb228081.kb.dk.warc, "
                        + "arc_full:193305-197-20131111175547-00001-kb228081.kb.dk.warc",
                        "193305-197-20131111175547-00001-kb228081.kb.dk.warc"
                },
                {
                        "arc_orig:kb, arc_harvesttime:2012-10-18T21:02:45.000Z, arc_job:36861, "
                        + "arc_name:kb-pligtsystem-36861-20121018210245-00000.warc, "
                        + "arc_full:kb-pligtsystem-36861-20121018210245-00000.warc",
                        "kb-pligtsystem-36861-20121018210245-00000.warc"
                },
                {
                        "arc_orig:metadata, "
                        + "arc_name:1298-metadata-2.arc, "
                        + "arc_full:anotherpath/1298-metadata-2.arc",
                        "anotherpath/1298-metadata-2.arc"
                },
                {
                        "arc_orig:unknown, "
                        + "arc_name:ksjvksjfvsk, "
                        + "arc_full:ksjvksjfvsk",
                        "ksjvksjfvsk"
                }
        }) {
            SolrRecord solr = new SolrRecord();
            ana.analyse(new FakeHeader(test[1]), null, solr);

            for (String expectedPair:test[0].split(" *, *")) {
                String[] tokens = expectedPair.split(":", 2);
                Object valO = solr.getFieldValue(tokens[0]);
                String value = valO == null ? "N/A" : valO.toString();
                assertEquals("The parsing of " + test[1] + " should have the right content for field " + tokens[0],
                             tokens[1], value);
            }
        }
    }



    private ARCNameAnalyser getAnalyser() {
        Config conf = ConfigFactory.parseURL(
                Thread.currentThread().getContextClassLoader().getResource("arcnameanalyser.conf"));
        return new ARCNameAnalyser(conf);
    }

    private class FakeHeader implements ArchiveRecordHeader {
        private final String arcPath;
        public FakeHeader(String arcPath) {
            this.arcPath = arcPath;
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
            return 0;
        }

        @Override
        public String getUrl() {
            return null;
        }

        @Override
        public String getMimetype() {
            return null;
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
            return null;
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