package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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

import picocli.CommandLine;
import picocli.CommandLine.Option;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecordHeader;

import java.util.HashMap;

/**
 * Config supporting factory for {@link SolrRecord}, making it possible to specify limits on Solr fields.
 */
public class SolrRecordFactory {
    private static Log log = LogFactory.getLog(SolrRecordFactory.class);

    public static final String KEY_DEFAULT_MAX = "warc.solr.field_setup.default_max_length";
    public static final String KEY_FIELD_LIST = "warc.solr.field_setup.fields";
    public static final String KEY_MAX = "max_length";

    public static final int DEFAULT_MAX_LENGTH = -1; // -1 = no limit

    @Option(names = "--default-max-len", defaultValue = "-1")
    private int defaultMax = DEFAULT_MAX_LENGTH;

    private HashMap<String, Integer> maxLengths = new HashMap<String, Integer>(); // Explicit
                                                                                  // HashMap
                                                                                  // as
                                                                                  // it
                                                                                  // is
                                                                                  // Serializable

    @Option(names = { "-MX", "--max-len" })
    public void setMaxLength(HashMap<String, Integer> maxLengths) {
        this.maxLengths = maxLengths;
        // And enumerate for printing:
        StringBuilder sb = new StringBuilder();
        for (String field : maxLengths.keySet()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(field).append("=").append(maxLengths.get(field));
        }
        log.info("Created SolrRecordFactory(defaultMaxLength=" + defaultMax
                + ", maxLengths=[" + sb + "])");
    }

    public static SolrRecordFactory createFactory(CommandLine cli) {
        return new SolrRecordFactory(cli);
    }

    private SolrRecordFactory(CommandLine cli) {
        // Register these options:
        cli.addMixin("SolrRecordFactory", this);
    }

    public SolrRecord createRecord() {
        return new SolrRecord(defaultMax, maxLengths);
    }

    public SolrRecord createRecord(String filename, ArchiveRecordHeader header) {
        return new SolrRecord(defaultMax, maxLengths, filename, header);
    }

}
