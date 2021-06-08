package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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

import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import org.archive.io.ArchiveRecordHeader;

import java.util.HashMap;
import java.util.Map;

/**
 * Config supporting factory for {@link SolrRecord}, making it possible to specify limits on Solr fields.
 */
public class SolrRecordFactory {
    private static Logger log = LoggerFactory.getLogger(SolrRecordFactory.class);

    public static final String KEY_DEFAULT_MAX = "warc.solr.field_setup.default_max_length";
    public static final String KEY_FIELD_LIST = "warc.solr.field_setup.fields";
    public static final String KEY_MAX = "max_length";

    public static final int DEFAULT_MAX_LENGTH = -1; // -1 = no limit

    private final int defaultMax;
    private final HashMap<String, Integer> maxLengths = new HashMap<>(); // Explicit HashMap as it is Serializable

    public static SolrRecordFactory createFactory(Config config) {
        return new SolrRecordFactory(config);
    }

    private SolrRecordFactory(Config config) {
        defaultMax = config != null && config.hasPath(KEY_DEFAULT_MAX) ?
                config.getInt(KEY_DEFAULT_MAX) : DEFAULT_MAX_LENGTH;
        StringBuilder sb = new StringBuilder();
        if (config != null && config.hasPath(KEY_FIELD_LIST)) {
            for (Map.Entry<String, ConfigValue> cv : config.getObject(KEY_FIELD_LIST).entrySet()) {
                String field = cv.getKey();
                String maxKey = KEY_FIELD_LIST + "." + field + "." + KEY_MAX;
                if (config.hasPath(maxKey)) {
                    long max = config.getBytes(KEY_FIELD_LIST + "." + field + "." + KEY_MAX);
                    if (max > Integer.MAX_VALUE) {
                        log.error("The limit " + max + " for field " + field + " exceeds Integer.MAX_VALUE");
                        continue;
                    }
                    if (sb.length() > 0) {
                        sb.append(", ");
                    }
                    sb.append(field).append("=").append(max);
                    maxLengths.put(field, (int) max);
                }
            }
        }
        log.info("Created SolrRecordFactory(defaultMaxLength=" + defaultMax + ", maxLengths=[" + sb + "])");
    }

    public SolrRecord createRecord() {
        return new SolrRecord(defaultMax, maxLengths);
    }

    public SolrRecord createRecord(String filename, ArchiveRecordHeader header) {
        return new SolrRecord(defaultMax, maxLengths, filename, header);
    }

}
