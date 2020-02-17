package uk.bl.wa.hadoop.mapreduce.cdx;

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
        implements CloseableIterator<String> {

    private Iterator<CaptureSearchResult> itr;
    private CDXFormat cdx;
    private long filesize;
    private CaptureSearchResult cachedPrev = null;
    private CaptureSearchResult cachedNext = null;

    public CaptureSearchResultIterator(Iterator<CaptureSearchResult> input,
            CDXFormat cdx, long filesize) {
        this.cdx = cdx;
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
    public String next() {
        if (cachedPrev == null) {
            throw new NoSuchElementException("call hasNext first!");
        }
        CaptureSearchResult o = cachedPrev;
        cachedPrev = null;
        return cdx.serializeResult(o);
    }

    @Override
    public void close() throws IOException {
    }
}