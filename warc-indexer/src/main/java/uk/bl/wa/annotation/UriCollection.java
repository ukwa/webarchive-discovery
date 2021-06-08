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

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 
 * Part of the @Annotations data model.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class UriCollection {

    @JsonProperty
    protected String collection;

    @JsonProperty
    protected String[] collections;

    @JsonProperty
    protected String[] subject;
    
    protected UriCollection() {
    }

    public UriCollection(String collection, String[] collections,
            String[] subjects) {
        this.collection = collection;
        this.collections = collections;
        this.subject = subjects;
    }

    public UriCollection(String collection, String collections,
            String subject) {
        if (collection != null && collection.length() > 0)
            this.collection = collection;
        if (collections != null && collections.length() > 0)
            this.collections = collections.split("\\s*\\|\\s*");
        if (subject != null && subject.length() > 0)
            this.subject = subject.split("\\s*\\|\\s*");
    }

    public String toString() {
        return collection + " : " + Arrays.toString(collections) + " : "
                + Arrays.toString(subject);
    }
}
