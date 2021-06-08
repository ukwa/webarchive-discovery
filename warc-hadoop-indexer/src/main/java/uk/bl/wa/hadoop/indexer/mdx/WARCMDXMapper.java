package uk.bl.wa.hadoop.indexer.mdx;

/*
 * #%L
 * warc-hadoop-indexer
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.common.SolrInputField;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.hadoop.indexer.WARCIndexerMapper;
import uk.bl.wa.hadoop.indexer.WritableSolrRecord;
import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

@SuppressWarnings( { "deprecation" } )
public class WARCMDXMapper extends MapReduceBase implements
        Mapper<Text, WritableArchiveRecord, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(WARCMDXMapper.class );

    private WARCIndexerMapper wim;

    public WARCMDXMapper() {
        try {
            // Re-configure logging:
            Properties props = new Properties();
            props.load(getClass().getResourceAsStream("/log4j-override.properties"));
            PropertyConfigurator.configure(props);
        } catch (IOException e1) {
            LOG.error("Failed to load log4j config from properties file.");
        }
    }

    @Override
    public void configure(JobConf job) {
        if (wim == null) {
            wim = new WARCIndexerMapper();
            wim.configure(job);
        }
    }

    @Override
    public void map(Text key, WritableArchiveRecord value,
            OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        // Use the main indexing code:
        WritableSolrRecord wsolr = wim.innerMap(key, value, reporter);

        // Ignore skipped records, where wsolr will be NULL:
        if (wsolr != null) {
            SolrRecord solr = wsolr.getSolrRecord();

            // Wrap up the result:
            MDX mdx;
            // Wrap up the key:
            Text oKey;
            try {
                mdx = fromWritableSolrRecord(solr);
                oKey = new Text(mdx.getHash());

                // Alternative key, based on record type + url + timestamp
                // Text oKey = new Text(mdx.getUrl() + "\t" + mdx.getTs() + "\t"
                // + mdx.getRecordType());

                // Collect
                Text result = new Text(mdx.toString());
                output.collect(oKey, result);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 
     * @param solr
     * @return
     * @throws JSONException
     */
    public static MDX fromWritableSolrRecord(SolrRecord solr)
            throws JSONException {
        MDX m = new MDX();
        m.setHash(stringValueOrUnset(solr.getFieldValue(SolrFields.HASH)));
        m.setUrl(stringValueOrUnset(solr.getFieldValue(SolrFields.SOLR_URL)));
        m.setTs(stringValueOrUnset(
                solr.getFieldValue(SolrFields.WAYBACK_DATE)));
        m.setRecordType(stringValueOrUnset(
                solr.getFieldValue(SolrFields.SOLR_RECORD_TYPE)));
        // Pass though Solr fields:
        for (String f : solr.getSolrDocument().getFieldNames()) {
            SolrInputField v = solr.getSolrDocument().get(f);
            if (v.getValueCount() > 1) {
                Iterator<Object> i = v.getValues().iterator();
                List<String> vals = new ArrayList<String>();
                while (i.hasNext()) {
                    vals.add(i.next().toString());
                }
                m.put(f, vals);
            } else {
                m.put(f, v.getFirstValue());
            }
        }

        return m;
    }

    private static String stringValueOrUnset(Object val) {
        if (val == null) {
            return "unset";
        } else {
            return val.toString();
        }
    }

}
