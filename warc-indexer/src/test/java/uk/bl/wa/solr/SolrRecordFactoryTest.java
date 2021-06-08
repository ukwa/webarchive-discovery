package uk.bl.wa.solr;

/*
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

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.impl.ConfigImpl;
import org.junit.Test;

import static org.junit.Assert.*;
import com.typesafe.config.Config;

import java.io.File;
import java.net.URL;

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
public class SolrRecordFactoryTest {

    @Test
    public void testBasicFactorySetup() {
        final String KEY_URL_MAX_LENGTH = "warc.solr.field_setup.fields.url.max_length";

        URL ref = Thread.currentThread().getContextClassLoader().getResource("reference.conf");
        assertNotNull("The config reference.conf should exist", ref);
        File configFilePath = new File(ref.getFile());
        Config conf = ConfigFactory.parseFile(configFilePath);
        assertTrue("Max for url field should be specified with key " + KEY_URL_MAX_LENGTH,
                   conf.hasPath(KEY_URL_MAX_LENGTH));
        SolrRecordFactory factory = SolrRecordFactory.createFactory(conf);

        {
            SolrRecord record = factory.createRecord();
            final String FAKE_URL = "short";
            record.addField("url", FAKE_URL);
            assertEquals("The length of the url field with a short String should be unchanged",
                         FAKE_URL.length(), record.getFieldValue("url").toString().length());
        }
        {
            SolrRecord record = factory.createRecord();
            StringBuilder fakeURL = new StringBuilder(4000);
            fakeURL.append("short");
            for (int i = 0 ; i < 2500 ; i++) {
                fakeURL.append("O");
            }
            record.addField("url", fakeURL.toString());
            assertEquals("The length of the url field with a huge String should be trimmed",
                         conf.getBytes(KEY_URL_MAX_LENGTH).intValue(),
                         record.getFieldValue("url").toString().length());
        }
    }
}
