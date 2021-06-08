/**
 * 
 */
package uk.bl.wa.annotation;

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * 
 * This is the data model for the annotations.
 * 
 * The collections hash-map collects the URI-to-Collection maps by scope, i.e.
 * HashMap<SCOPE, HashMap<URI, COLLECTION>>
 * 
 * The collectionDateRanges refines this by noting the date restriction for each
 * top-level collection.
 * 
 * Currently supports Subjects, Collections and associated date ranges.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class Annotations {
    private static final Logger LOG = LoggerFactory.getLogger(Annotations.class);

    @JsonProperty
    private HashMap<String, HashMap<String, UriCollection>> collections;

    @JsonProperty
    private HashMap<String, DateRange> collectionDateRanges;

    public Annotations() {
        // Initialise the collection maps:
        collections = new HashMap<String, HashMap<String, UriCollection>>();
        collections.put("resource", new HashMap<String, UriCollection>());
        collections.put("plus1", new HashMap<String, UriCollection>());
        collections.put("root", new HashMap<String, UriCollection>());
        collections.put("subdomains", new HashMap<String, UriCollection>());
        collections.put("source_file_matches", new HashMap<String, UriCollection>());

        // An the date ranges:
        collectionDateRanges = new HashMap<String, DateRange>();
    }

    public HashMap<String, HashMap<String, UriCollection>> getCollections() {
        return this.collections;
    }

    public HashMap<String, DateRange> getCollectionDateRanges() {
        return this.collectionDateRanges;
    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                false);
        return mapper;
    }

    /**
     * 
     * @param ann
     * @return
     */
    public String toJson() {
        try {
            ObjectMapper mapper = getObjectMapper();
            return mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 
     * @param filename
     * @throws FileNotFoundException
     */
    public void toJsonFile(String filename) throws FileNotFoundException {
        final OutputStream os = new FileOutputStream(filename);
        final PrintStream printStream = new PrintStream(os);
        printStream.print(this.toJson());
        printStream.close();
    }

    /**
     * 
     * @param json
     * @return
     * @throws IOException
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public static Annotations fromJson(String json) throws JsonParseException,
            JsonMappingException, IOException {
        ObjectMapper mapper = getObjectMapper();
        try {
            return mapper.readValue(json, Annotations.class);
        } catch (JsonParseException e) {
            LOG.error("JsonParseException: " + e, e);
            LOG.error("When parsing: " + json);
            throw e;
        }
    }

    /**
     * 
     * @param filename
     * @return
     * @throws IOException
     * @throws JsonMappingException
     * @throws JsonParseException
     */
    public static Annotations fromJsonFile(String filename)
            throws JsonParseException, JsonMappingException, IOException {
        return fromJson(FileUtils.readFileToString(new File(filename)));
    }

}
