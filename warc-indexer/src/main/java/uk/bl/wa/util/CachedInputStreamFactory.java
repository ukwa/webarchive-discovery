/**
 * 
 */
package uk.bl.wa.util;

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

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jwat.common.RandomAccessFileInputStream;

import java.io.*;

/**
 * Utility method that takes a given input stream and caches the content in RAM, on disk, based on some size limits.
 *
 * This allows for cheap calls to {@link InputStream#reset()} from the generated InputStream.
 */
public class CachedInputStreamFactory {
    private static Logger log = LoggerFactory.getLogger(CachedInputStreamFactory.class );

    // Thresholds:
    public static final long defaultInMemoryThreshold = 1024*1024;     // Up to 1MB allowed in RAM.
    public static final long defaultOnDiskThreshold =   1024*1024*100; // Up to 100MB cached on disk.

    /**
     * Reads the content from in to a memory- or disk-cache, depending on thresholds.
     * Returns the cached bytes as an InputStream that supports {@link InputStream#mark(int)} up to length bytes.
     *
     * Note: It is important to call {@link InputStream#close()} after the returned InputStream has been used, to
     * avoid temporary files building up.
     *
     * @param in any InputStream
     * @param length the number of bytes that the in is expected to hold.
     * @param onlyReadLength if true, only length bytes will be read, else reading will continue to EOF.
     * @param closeAfterRead if true, in will be closed after reading.
     * @return an InputStream with the content of in  that supports {@link InputStream#mark(int)}.
     */
    public static InputStream cacheContent(
            InputStream in, long length, boolean onlyReadLength, boolean closeAfterRead) {
        return cacheContent(
                in, length, onlyReadLength, closeAfterRead, defaultInMemoryThreshold, defaultOnDiskThreshold);
    }

    /**
     * Reads the content from in to a memory- or disk-cache, depending on thresholds.
     * Returns the cached bytes as an InputStream that supports {@link InputStream#mark(int)} up to length bytes.
     *
     * Note: It is important to call {@link InputStream#close()} after the returned InputStream has been used, to
     * avoid temporary files building up.
     *
     * @param in any InputStream
     * @param length the number of bytes that the in is expected to hold.
     * @param onlyReadLength if true, only length bytes will be read, else reading will continue to EOF.
     * @param closeAfterRead if true, in will be closed after reading length bytes.
     * @param inMemoryThreshold if length is below this threshold, memory caching will be used, else disk caching will
     *                          be used.
     * @param onDiskThreshold if disk caching is used and length is above onDiskThresHold, only onDiskThreshold will be
     *                        stored on disk, while the remainder will be discarded (it will still be read).
     * @return an InputStream with the content of in that supports {@link InputStream#mark(int)}.
     */
    public static InputStream cacheContent(InputStream in, long length, boolean onlyReadLength, boolean closeAfterRead,
                                           long inMemoryThreshold, long onDiskThreshold) {
        try {
            return length < inMemoryThreshold ?
                    memoryCacheContent(in, (int) length, onlyReadLength, closeAfterRead, (int) inMemoryThreshold, onDiskThreshold) : // Always below 2GB
                    diskCacheContent(in, length, onlyReadLength , closeAfterRead, onDiskThreshold);
        } catch (IOException e) {
            log.error("Unable to cache InputStream, returning empty stream", e);
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    // Note: if onlyReadLength is false and reading exceeds inMemoryThreshold, flow will be switched to diskCacheContent
    private static InputStream memoryCacheContent(
            InputStream in, int length, boolean onlyReadLength, boolean closeAfterRead,
            int inMemoryThreshold, long onDiskThreshold) throws IOException {
        ByteArrayOutputStream cache = new ByteArrayOutputStream(length);
        long copied = IOUtils.copyLarge(in, cache, 0, length);
        if (copied == length && !onlyReadLength) { // Don't know if EOF is reached and we're allowed to read further
                if (copied < inMemoryThreshold) {
                    copied += IOUtils.copyLarge(in, cache, 0, inMemoryThreshold-copied);
                }
                if (copied == inMemoryThreshold) { // Still don't know if EOF, but memory limit has been reached
                    log.debug("Attempted to memory cache content but limit of " + inMemoryThreshold + " bytes has " +
                              "been reached. Switching to disk based caching");
                    return diskCacheContent(
                            new SequenceInputStream(new ByteArrayInputStream(cache.toByteArray()), in) {
                                @Override
                                public void close() throws IOException {
                                    super.close();
                                    in.close();
                                }
                            }, inMemoryThreshold, false, closeAfterRead, onDiskThreshold);
                }
        }

        if (closeAfterRead) {
            in.close();
        }
        return new ByteArrayInputStream(cache.toByteArray());
    }

    private static InputStream diskCacheContent(
            InputStream in, long length, boolean onlyReadLength, boolean closeAfterRead,
            long onDiskThreshold) throws IOException {
        File cacheFile = File.createTempFile("warc-indexer", ".cache");
        cacheFile.deleteOnExit();
        OutputStream cache = new FileOutputStream(cacheFile);

        long toCopy = onlyReadLength ? Math.min(length, onDiskThreshold) : onDiskThreshold;
        long copied = IOUtils.copyLarge(in, cache, 0, toCopy);
        if (closeAfterRead) {
            in.close();
        }
        cache.close();

        if (onlyReadLength && length > onDiskThreshold) {
            // Read the remainder of the stream
            log.debug(String.format("diskCacheContent(length=%d, onDiskThreshold=%d) will be truncated",
                                    length, onDiskThreshold));
            // IOUtils.skip is not a skip: It reads the bytes from the stream, which is what we need
            IOUtils.skip(in, length - onDiskThreshold);
        } else if (!onlyReadLength && copied == onDiskThreshold) { // Don't know if EOF has been reached
            // Read on ad infinitum
            IOUtils.skip(in, Long.MAX_VALUE);
        }

        return new RandomAccessFileInputStream(new RandomAccessFile(cacheFile, "r")) {
            @Override
            public void close() throws IOException {
                try {
                    if (raf != null) { // It will be null if it is already closed
                        raf.close();
                    }
                } catch (Exception e) {
                    log.warn("Exception closing RandomAccessFile cache for '" + cacheFile + "'", e);
                }
                super.close(); // Must be after raf.close() as it sets raf = null
                if(cacheFile.exists()) {
                    try {
                        if (!cacheFile.delete()) {
                            log.warn("Unable to delete '" + cacheFile + "'");
                        }
                    } catch (Exception e) {
                        log.warn("Exception deleting '" + cacheFile + "'", e);
                    }
                }

            }
        };
    }
}
