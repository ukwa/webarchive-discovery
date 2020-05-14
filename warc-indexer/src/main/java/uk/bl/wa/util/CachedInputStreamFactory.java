/**
 * 
 */
package uk.bl.wa.util;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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

import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.Base32;
import org.jwat.common.RandomAccessFileInputStream;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_PAYLOAD_DIGEST;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

/**
 * Utility method that takes a given input stream and caches the content in RAM, on disk, based on some size limits.
 *
 * This allows for cheap calls to {@link InputStream#reset()} from the input stream provided by {@link #getInputStream()}.
 */
public class CachedInputStreamFactory {
    private static Log log = LogFactory.getLog( CachedInputStreamFactory.class );

    // Thresholds:
    public static final long defaultInMemoryThreshold = 1024*1024;     // Up to 1MB allowed in RAM.
    public static final long defaultOnDiskThreshold =   1024*1024*100; // Up to 100MB cached on disk.

    /**
     * Reads length bytes from in to a memory- or disk-cache, depending on thresholds.
     * Returns the cached bytes as an InputStream that supports {@link InputStream#mark(int)} up to length bytes.
     *
     * Note: It is important to call {@link InputStream#close()} after the returned InputStream has been used, to
     * avoid temporary files building up.
     *
     * @param in any InputStream
     * @param length the number of bytes to read from in.
     * @param closeAfterRead if true, in will be closed after reading length bytes.
     * @return an InputStream with the content of in  that supports {@link InputStream#mark(int)} up to length bytes.
     */
    public static InputStream cacheContent(InputStream in, long length, boolean closeAfterRead) {
        return cacheContent(in, length, closeAfterRead, defaultInMemoryThreshold, defaultOnDiskThreshold);
    }

    /**
     * Reads length bytes from in to a memory- or disk-cache, depending on thresholds.
     * Returns the cached bytes as an InputStream that supports {@link InputStream#mark(int)} up to length bytes.
     *
     * Note: It is important to call {@link InputStream#close()} after the returned InputStream has been used, to
     * avoid temporary files building up.
     *
     * @param in any InputStream
     * @param length the number of bytes to read from in.
     * @param closeAfterRead if true, in will be closed after reading length bytes.
     * @param inMemoryThreshold if length is below this threshold, memory caching will be used, else disk caching will
     *                          be used.
     * @param onDiskThreshold if disk caching is used and length is above onDiskThresHold, only onDiskThreshold will be
     *                        stored on disk, while the remainder will be discarded (it will still be read).
     * @return an InputStream with the content of in  that supports {@link InputStream#mark(int)} up to length bytes.
     */
    public static InputStream cacheContent(InputStream in, long length, boolean closeAfterRead,
                                           long inMemoryThreshold, long onDiskThreshold) {
        try {
            return length < inMemoryThreshold ?
                    memoryCacheContent(in, (int) length, closeAfterRead) : // Always below 2GB
                    diskCacheContent(in, length, closeAfterRead, onDiskThreshold);
        } catch (IOException e) {
            log.error("Unable to cache InputStream, returning empty stream", e);
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    private static InputStream memoryCacheContent(
            InputStream in, int length, boolean closeAfterRead) throws IOException {
        ByteArrayOutputStream cache = new ByteArrayOutputStream(length);
        IOUtils.copyLarge(in, cache, 0, length);
        if (closeAfterRead) {
            in.close();
        }
        return new ByteArrayInputStream(cache.toByteArray());
    }

    private static InputStream diskCacheContent(
            InputStream in, long length, boolean closeAfterRead, long onDiskThreshold) throws IOException {
        File cacheFile = File.createTempFile("warc-indexer", ".cache");
        cacheFile.deleteOnExit();
        OutputStream cache = new FileOutputStream(cacheFile);

        long toCopy = Math.min(length, onDiskThreshold);
        IOUtils.copyLarge(in, cache, 0, toCopy);
        if (closeAfterRead) {
            in.close();
        }
        cache.close();

        boolean truncated = length > onDiskThreshold;
        // Read the remainder of the stream
        if(truncated) {
            log.debug(String.format("diskCacheContent(length=%d, onDiskThreshold=%d) will be truncated",
                                    length, onDiskThreshold));
            // IOUtils.skip is not a skip: It reads the bytes from the stream, which is what we need
            IOUtils.skip(in, length - onDiskThreshold);
        }

        return new RandomAccessFileInputStream(new RandomAccessFile(cacheFile, "r")) {
            @Override
            public void close() throws IOException {
                super.close();
                try {
                    raf.close();
                } catch (Exception e) {
                    log.warn("Exception closing RandomAccessFile cache for '" + cacheFile + "'", e);
                }
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
