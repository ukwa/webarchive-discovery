package uk.bl.wa;

/*-
 * #%L
 * warc-hadoop-indexer
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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Date;
import java.util.HashMap;


/**
 * Class used to build up metadata about a Memento.
 * Only a few critical fields are explicit, with metadata held in other fields.
 * Only limited metadata types are supported.
 */
public class MementoRecord implements Serializable {    

    private String sourceFilePath;
    
    private long sourceFileOffset;

    private SortedMap<String,Object> metadata = new TreeMap<String,Object>();
    private SortedMap<String,Class> metadataTypes = new TreeMap<String,Class>();

    public MementoRecord() {
    }

    public String getSourceFilePath() {
        return sourceFilePath;
    }


    public void setSourceFilePath(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }


    public long getSourceFileOffset() {
        return sourceFileOffset;
    }


    public void setSourceFileOffset(long sourceFileOffset) {
        this.sourceFileOffset = sourceFileOffset;
    }

    public void addMetadata(String name, String value) {
        this.metadata.put(name, value);
        this.metadataTypes.put(name, String.class);
    }

    public SortedMap<String, Object> getMetadata() {
        return this.metadata;
    }

    public SortedMap<String, Class> getMetadataTypes() {
        return this.metadataTypes;
    }

}
