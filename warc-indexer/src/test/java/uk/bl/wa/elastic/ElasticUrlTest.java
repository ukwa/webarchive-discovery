package uk.bl.wa.elastic;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


public class ElasticUrlTest {

    @Test
    public void testInvalidUrls() {
    	ElasticUrl eu;
    	
    	eu = new ElasticUrl(null);
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("invalid");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("ftp://server");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("http://");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("https://");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("http://server");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("https://server");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("http://server");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("https://server/");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("http://server:x");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("https://server:x");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("http://server:");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("https://server:");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("http://server:9200");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("https://server:9200");
    	assertFalse(eu.isValid());

    	eu = new ElasticUrl("http://server:9200index");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("https://server:9200index");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("http://server:9200/");
    	assertFalse(eu.isValid());
    	
    	eu = new ElasticUrl("https://server:9200/");
    	assertFalse(eu.isValid());
    }
    
    @Test
    public void testValidUrls() {
    	ElasticUrl eu;
    	
    	eu = new ElasticUrl("http://server:9200/index");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTP);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 9200);
    	assertEquals(eu.getIndexName(), "index");
    	
    	eu = new ElasticUrl("https://server:9200/index");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTPS);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 9200);
    	assertEquals(eu.getIndexName(), "index");

    	eu = new ElasticUrl("http://server/index");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTP);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 80);
    	assertEquals(eu.getIndexName(), "index");
    	
    	eu = new ElasticUrl("https://server/index");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTPS);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 80);
    	assertEquals(eu.getIndexName(), "index");

    	eu = new ElasticUrl("https://server:9200/index/");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTPS);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 9200);
    	assertEquals(eu.getIndexName(), "index");

    	eu = new ElasticUrl("http://server:9200/index/");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTP);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 9200);
    	assertEquals(eu.getIndexName(), "index");
    	
    	eu = new ElasticUrl("https://server/index/");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTPS);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 80);
    	assertEquals(eu.getIndexName(), "index");
    	
    	eu = new ElasticUrl("http://server/index/");
    	assertTrue(eu.isValid());
    	assertEquals(eu.getScheme(), ElasticUrl.HTTP);
    	assertEquals(eu.getServer(), "server");
    	assertEquals(eu.getPort(), 80);
    	assertEquals(eu.getIndexName(), "index");
    	
    }
}
