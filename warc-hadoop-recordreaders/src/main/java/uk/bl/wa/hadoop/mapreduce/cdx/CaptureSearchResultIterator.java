package uk.bl.wa.hadoop.mapreduce.cdx;

/*-
 * #%L
 * warc-hadoop-recordreaders
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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;

/**
 * 
 * An alternative iterator that ensure we have a record length as well as
 * offset.
 * 
 * A bit of a hack, as it calculates this based on the offset of the previous
 * records, and so over-estimates the length for WARC records because by default
 * the indexer skips request and metadata records.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class CaptureSearchResultIterator
        implements CloseableIterator<CaptureSearchResult> {

    private Iterator<CaptureSearchResult> itr;
    private long filesize;
    private CaptureSearchResult cachedPrev = null;
    private CaptureSearchResult cachedNext = null;

    public CaptureSearchResultIterator(Iterator<CaptureSearchResult> input,
            long filesize) {
        this.itr = input;
        this.filesize = filesize;
    }

    @Override
    public boolean hasNext() {
        if (cachedPrev != null)
            return true;
        while (itr.hasNext()) {
            if (cachedNext != null) {
                cachedPrev = this.cachedNext;
            }
            cachedNext = itr.next();
            // Calculate the offset:
            if (cachedPrev != null) {
                cachedPrev.setCompressedLength(this.cachedNext.getOffset()
                        - this.cachedPrev.getOffset());
                return true;
            }
        }
        if (cachedNext != null) {
            cachedPrev = cachedNext;
            cachedPrev.setCompressedLength(filesize - cachedPrev.getOffset());
            cachedNext = null;
            return true;
        }
        return false;
    }

    @Override
    public CaptureSearchResult next() {
        if (cachedPrev == null) {
            throw new NoSuchElementException("call hasNext first!");
        }
        CaptureSearchResult o = cachedPrev;
        cachedPrev = null;
        return o;
    }

    @Override
    public void close() throws IOException {
    }
}
