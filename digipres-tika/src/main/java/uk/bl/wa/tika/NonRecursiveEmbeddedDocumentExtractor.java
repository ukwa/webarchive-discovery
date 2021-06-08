/**
 * 
 */
package uk.bl.wa.tika;

/*
 * #%L
 * digipres-tika
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

import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

/**
 * Override embedded document parser logic to prevent descent.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class NonRecursiveEmbeddedDocumentExtractor extends
        ParsingEmbeddedDocumentExtractor {

    /** Parse embedded documents? Defaults to TRUE */
    private boolean parseEmbedded = true;

    public NonRecursiveEmbeddedDocumentExtractor(ParseContext context) {
        super(context);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor#
     * shouldParseEmbedded(org.apache.tika.metadata.Metadata)
     */
    @Override
    public boolean shouldParseEmbedded(Metadata metadata) {
        return this.parseEmbedded;
    }

    /**
     * @return the parseEmbedded
     */
    public boolean isParseEmbedded() {
        return parseEmbedded;
    }

    /**
     * @param parseEmbedded
     *            the parseEmbedded to set
     */
    public void setParseEmbedded(boolean parseEmbedded) {
        this.parseEmbedded = parseEmbedded;
    }

}
