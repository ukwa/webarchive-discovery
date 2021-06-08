package uk.bl.wa.util;

/*-
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.ArchiveUtils;
import org.apache.commons.httpclient.ChunkedInputStream;
import uk.bl.wa.indexer.HTTPHeader;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_PAYLOAD_DIGEST;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

public class InputStreamUtils {
    private static Logger log = LoggerFactory.getLogger(InputStreamUtils.class );

    /**
     * If true, {@link #maybeDechunk} also accepts LF as terminator for hex strings, instead of only CRLF.
     */
    public static final boolean LENIENT_DECHUNK = true;

    /**
     * If no explicit hashStage is stated, this will be used. This stage matches the warc-1.1 specification
     * http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-payload-digest
     */
    public static final HASH_STAGE DEFAULT_HASH_STAGE = HASH_STAGE.after_dechunk_before_decompression;

    /**
     * The WARC standard at https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-payload-digest
     * does not mention chunking and compression explicitly, but it does state that the {@code WARC-Payload-Digest}
     * operates on the "logical record". Chunking is a transfer detail and compression is an encoding detail, both of
     * which can be see and independent of the logical record.
     *
     * The HASH_STAGE represents the three possible places in the processing stream to perform the hashing.
     */
    public enum HASH_STAGE {
        /**
         * The hashing will be calculated over the WARC-payload bytes as-is.
         */
        first,
        /**
         * The hashing will be calculated after dechunking, but before decompression.
         */
        after_dechunk_before_decompression,
        /**
         * The hashing will be calculated after dechunking and after decompression.
         */
        after_dechunk_after_decompression}

    /**
     * Calculates SHA-1 hash from length bytes of input, performs decompression & dechunking of the content and
     * returns the resulting content as a stream that supports {@link InputStream#mark(int)} up to length.
     * The hash digestion is performed directly on the bytes from input, before decompression & dechunking.
     * Dechunking is performed before decompression. Hashing is done after dechunking but before decompression
     * as per {@link #DEFAULT_HASH_STAGE}.
     * @param input any InputStream.
     * @param length the number of bytes to read from input.
     * @param url the URL for the content. Used for log messages.
     * @param warcHeader will be used to derive expected hash.
     * @param httpHeader will be used to extract hints for compression and chunking.
     * @param inMemoryThreshold if length is below this threshold, memory caching will be used, else disk caching will
     *                          be used.
     * @param onDiskThreshold if disk caching is used and length is above onDiskThresHold, only onDiskThreshold will be
     *                        stored on disk, while the remainder will be discarded (it will still be read).
     * @return a simple structure containing the hash information and the cached decompressed dechunked content.
     */
    public static HashIS cacheDecompressDechunkHash(
            InputStream input, long length, String url, ArchiveRecordHeader warcHeader,
            HTTPHeader httpHeader, long inMemoryThreshold, long onDiskThreshold) throws IOException {
        return cacheDecompressDechunkHash(
                input, length, url, warcHeader, httpHeader, inMemoryThreshold, onDiskThreshold, DEFAULT_HASH_STAGE);
    }

    /**
     * Calculates SHA-1 hash from length bytes of input, performs decompression & dechunking of the content and
     * returns the resulting content as a stream that supports {@link InputStream#mark(int)} up to length.
     * Dechunking is performed before decompression.
     * @param input any InputStream.
     * @param length the number of bytes to read from input.
     * @param url the URL for the content. Used for log messages.
     * @param warcHeader will be used to derive expected hash.
     * @param httpHeader will be used to extract hints for compression and chunking.
     * @param inMemoryThreshold if length is below this threshold, memory caching will be used, else disk caching will
     *                          be used.
     * @param onDiskThreshold if disk caching is used and length is above onDiskThresHold, only onDiskThreshold will be
     *                        stored on disk, while the remainder will be discarded (it will still be read).
     * @param hashSTage where in the delivery chain hashing is performed.
     * @return a simple structure containing the hash information and the cached decompressed dechunked content.
     */
    public static HashIS cacheDecompressDechunkHash(
            InputStream input, long length, String url, ArchiveRecordHeader warcHeader,
            HTTPHeader httpHeader, long inMemoryThreshold, long onDiskThreshold, HASH_STAGE hashSTage)
            throws IOException {
        String expectedHash =
                (warcHeader == null || !warcHeader.getHeaderFieldKeys().contains(HEADER_KEY_PAYLOAD_DIGEST))?
                        null :
                        Normalisation.sha1HashAsBase32((String) warcHeader.getHeaderValue(HEADER_KEY_PAYLOAD_DIGEST));
        boolean checkHash =
                warcHeader != null && warcHeader.getHeaderFieldKeys().contains(HEADER_KEY_TYPE) &&
                warcHeader.getHeaderValue(HEADER_KEY_TYPE).equals(WARCConstants.WARCRecordType.response.toString());
        String compressionHint = httpHeader == null ? null : httpHeader.getHeader("Content-Encoding", null);
        String chunkHint = httpHeader == null ? null : httpHeader.getHeader("Transfer-Encoding", null);
        return cacheDecompressDechunkHash(input, length, url, expectedHash, checkHash,
                                          compressionHint, chunkHint, inMemoryThreshold, onDiskThreshold, hashSTage);
    }

    /**
     * Calculates SHA-1 hash from length bytes of input, performs decompression & dechunking of the content and
     * returns the resulting content as a stream that supports {@link InputStream#mark(int)} up to length.
     * Dechunking is performed before decompression.
     * Note: The final size of the content will normally exceed length if compression is used.
     * @param input any InputStream.
     * @param length the number of bytes to read from input.
     * @param url the URL for the content. Used for log messages.
     * @param expectedHash will be compared with the calculated hash.
     * @param checkHash if true, the expectedHash will be compared with the calculated, if false the expectedHash
     *                  will override the calculated.
     * @param compressionHint {@code brotli}, {@code gz} or null.
     *                        Will be used with {@link #maybeDecompress(InputStream, String)}.
     *                        Normally taken from the HTTP-header {@code Content-Encoding}.
     * @param chunkHint       {@code chunked} or null.
     *                        Will be used with {@link #maybeDechunk(InputStream, String)}.
     *                        Normally taken from the HTTP-header {@code Transfer-Encoding}.
     * @param inMemoryThreshold if length is below this threshold, memory caching will be used, else disk caching will
     *                          be used.
     * @param onDiskThreshold if disk caching is used and length is above onDiskThresHold, only onDiskThreshold will be
     *                        stored on disk, while the remainder will be discarded (it will still be read).
     * @param hashSTage where in the delivery chain hashing is performed.
     * @return a simple structure containing the hash information and the cached decompressed dechunked content.
     */
    public static HashIS cacheDecompressDechunkHash(
            InputStream input, long length, String url, String expectedHash, boolean checkHash,
            String compressionHint, String chunkHint, long inMemoryThreshold, long onDiskThreshold, HASH_STAGE hashSTage)
            throws IOException {
        String shortURL = url != null && url.length() > 200 ? url.substring(0, 200) + "..." : url;
        switch (hashSTage) {
            case first: {
                HashedInputStream hash = new HashedInputStream(url, expectedHash, checkHash, input, length);
                InputStream stream = CachedInputStreamFactory.cacheContent(
                        // Don't try to de-chunk or de-compress if length is 0
                        length == 0 ? hash : maybeDecompress(maybeDechunk(hash, chunkHint, shortURL), compressionHint),
                        length, false, true, inMemoryThreshold, onDiskThreshold);
                return new HashIS(stream, hash);
            }
            case after_dechunk_before_decompression: {
                InputStream dechunked = length == 0 ? input : maybeDechunk(input, chunkHint, shortURL);
                HashedInputStream hash = new HashedInputStream(url, expectedHash, checkHash, dechunked, length);
                InputStream stream = CachedInputStreamFactory.cacheContent(
                        // Don't try to de-chunk or de-compress if length is 0
                        length == 0 ? hash : maybeDecompress(hash, compressionHint),
                        length, false, true, inMemoryThreshold, onDiskThreshold);
                return new HashIS(stream, hash);
            }
            case after_dechunk_after_decompression: {
                InputStream deall = length == 0 ? input :
                        maybeDecompress(maybeDechunk(input, chunkHint, shortURL), compressionHint);
                HashedInputStream hash = new HashedInputStream(url, expectedHash, checkHash, deall, length);
                InputStream stream = CachedInputStreamFactory.cacheContent(
                        deall, length, false, true, inMemoryThreshold, onDiskThreshold);
                return new HashIS(stream, hash);
            }
            default: throw new IllegalArgumentException(
                    "Unknown HASH_STAGE '" + hashSTage + "'. Valid values are " + Arrays.toString(HASH_STAGE.values()));
        }
    }

    /**
     * Contains a hash for the content and an InputStream with the content that supports {@link InputStream#mark(int)}.
     */
    public static class HashIS {
        private final InputStream is;
        private final HashedInputStream hashStream;

        public HashIS(InputStream is, HashedInputStream hashStream) {
            this.is = is;
            this.hashStream = hashStream;
        }

        /**
         * @return an InputStream with the content that supports {@link InputStream#mark(int)}.
         */
        public InputStream getInputStream() {
            return is;
        }

        /**
         * @return a emptied and closed stream containing information on hashing.
         */
        public HashedInputStream getHashStream() {
            return hashStream;
        }

        /**
         * Closes the inner InputStream with the content.
         * Important: This should be called after processing to avoid build up of temporary files.
         */
        public void cleanup() {
            try {
                is.close();
            } catch (IOException e) {
                log.warn("Exception closing inner InputStream. Probably not fatal as we are closing down", e);
            }
        }
    }

    /**
     * If chunkHint is {@code "chunked"}, this will redurect to {@link #maybeDechunk(InputStream)}, else input
     * will be returned unmodified.
     * @param input a stream with the response body from a HTTP-response.
     * @param chunkHint       {@code chunked} or null.
     *                        Normally taken from the HTTP-header {@code Transfer-Encoding}.
     * @return the un-chunked content of the given stream.
     * @throws IOException if the stream could not be processed.
     */
    public static InputStream maybeDechunk(InputStream input, String chunkHint) throws IOException {
        return maybeDechunk(input, chunkHint, "n/a");
    }

    /**
     * If chunkHint is {@code "chunked"}, this will redurect to {@link #maybeDechunk(InputStream)}, else input
     * will be returned unmodified.
     * @param input a stream with the response body from a HTTP-response.
     * @param chunkHint       {@code chunked} or null.
     *                        Normally taken from the HTTP-header {@code Transfer-Encoding}.
     * @param id the designation/name/id of the content. Only used for logging
     * @return the un-chunked content of the given stream.
     * @throws IOException if the stream could not be processed.
     */
    public static InputStream maybeDechunk(InputStream input, String chunkHint, String id) throws IOException {
        return "chunked".equalsIgnoreCase(chunkHint) ? maybeDechunkNamed(input, id) : input;
    }

    /**
     * Checks if an input stream seems to be chunked. If so, the stream content is de-chunked.
     * If not, the stream content is returned unmodified.
     * Chunked streams must begin with {@code ^[0-9a-z]{1,8}(;.{0,1024})?\r\n}.
     * Note: Closing the returned stream will automatically close input.
     * @param input a stream with the response body from a HTTP-response.
     * @return the un-chunked content of the given stream.
     * @throws IOException if the stream could not be processed.
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding">Transfer-Encoding</a>
     */
    public static InputStream maybeDechunk(InputStream input) throws IOException {
        return maybeDechunkNamed(input, "n/a");
    }
    /**
     * Checks if an input stream seems to be chunked. If so, the stream content is de-chunked.
     * If not, the stream content is returned unmodified.
     * Chunked streams must begin with {@code ^[0-9a-z]{1,8}(;.{0,1024})?\r\n}.
     * Note: Closing the returned stream will automatically close input.
     * @param input a stream with the response body from a HTTP-response.
     * @param id the designation/name/id of the content. Only used for logging
     * @return the un-chunked content of the given stream.
     * @throws IOException if the stream could not be processed.
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding">Transfer-Encoding</a>
     */
    public static InputStream maybeDechunkNamed(InputStream input, String id) throws IOException {
        final BufferedInputStream buf = new BufferedInputStream(input) {
            @Override
            public void close() throws IOException {
                super.close();
                input.close();
            }
        };
        buf.mark(1024); // Room for a lot of comments
        int pos = 0;
        int c = -1;
        // Check for hex-number
        while (pos <= 8) { // Max 8 digits + the character after
            c = buf.read();
            if (c== -1) { // EOF
                log.debug("maybeDechunk reached EOF while looking for hex digits at pos " + pos + ": " +
                          "Not a chunked stream, returning content as-is for " + id);
                buf.reset();
                return buf;
            }
            if (('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')) {
                pos++;
                continue;
            }
            break;
        }
        if (pos == 0 || pos > 8) {
            log.debug("maybeDechunk found " + pos + " hex digits: Not a chunked stream, returning content as-is for " + id);
            buf.reset();
            return buf;
        }
        // Check for \r\n or extension
        if (c == -1) { // EOF
            log.debug("maybeDechunk reached EOF while looking for extension or \\r\\n at pos " + pos + ": " +
                      "Not a chunked stream, returning content as-is for " + id);
            buf.reset();
            return buf;
        }
        pos++;
        if (c == ';') { // Extension
            while (pos < 1024) {
                while (pos < 1024 && c != '\r' && c != -1) { // Look for CR
                    c = buf.read();
                    pos++;
                }
                if (c == -1) {
                    break;
                }
                c = buf.read();
                pos++;
                if (c == '\n' || c == -1) { // LF
                    break;
                }
            }
            if (pos == 1024 || c == -1) {
                log.info("maybeDechunk found hex digits and start of an extension but could not locate CRLF: " +
                          "Not a chunked stream, returning content as-is for " + id);
                buf.reset();
                return buf;
            }
            log.debug("maybeDechunk found hex digits and an extension: Probably chunked stream, returning content " +
                      "wrapped in a de-chunker for " + id);
            return dechunk(buf);
        }
        // Not with extension. Next chars must be CRLF
        if (c == '\r') { // CR
            c = buf.read();
            if (c == '\n') { // LF
                log.debug("maybeDechunk found hex digits CRLF: Probably chunked stream, returning content " +
                          "wrapped in a de-chunker");
                return dechunk(buf);
            }
            log.info("maybeDechunk found hex digits followed by CR (0x" + Integer.toHexString('\r') +
                     ") and but the charactor following that was 0x" + Integer.toHexString(c) +
                     " and not LF (" + Integer.toHexString('\n') + ") for " + id);
        } else if (c == '\n') {
            if (LENIENT_DECHUNK) {
                log.debug("maybeDechunk found hex digits followed by LF (0x" + Integer.toHexString('\n') +
                         ") but expected CRLF. This is likely chunking delivered by a non-standard compliant server." +
                         " The de-chunker is lenient and accepts this for " + id);
                return dechunk(buf);
            }
            log.info("maybeDechunk found hex digits followed by LF (0x" + Integer.toHexString('\n') +
                     ") but expected CRLF. This is likely chunking delivered by a non-standard compliant server." +
                     " The de-chunker is not lenient and will return the stream as-is for " + id);
        } else {
            log.info("maybeDechunk found hex digits but could not locate CRLF. Instead it found 0x" +
                     Integer.toHexString(c) + ": Not a chunked stream, returning content as-is for " + id);
        }
        buf.reset();
        return buf;
    }

    private static ChunkedInputStream dechunk(InputStream in) throws IOException {
        in.reset();
        return new ChunkedInputStream(in) {
            @Override
            public void close() throws IOException {
                super.close();
                in.close();
            }
        };
    }

    /*
     * Provides a decompressing wrapper for the input. If the compressionHint is null, the method will attempt to
     * auto-guess if the input is GZip-compressed. Auto-guessing does not work for Brotli as such detection is
     * unreliable for that format.
     * If the hint is empty, the input stream will be returned unmodified.
     * Note: Closing the returned stream will automatically close input.
     * @param input the input stream that might be decompressed.
     * @param compressionHint if present, this will be used for selecting the compression scheme.
     *       Usable values are 'GZip' and 'Br'. Not case-sensitive.
     */
    public static InputStream maybeDecompress(InputStream input, String compressionHint) throws IOException {
        final String hint = compressionHint == null ? null : compressionHint.toLowerCase().trim();
        if (hint == null) { // Auto-guess
            // Detecting Brotli is hard: https://stackoverflow.com/questions/39008957/is-there-a-way-to-check-if-a-buffer-is-in-brotli-compressed-format
            final BufferedInputStream buffer = new BufferedInputStream(input) {
                @Override
                public void close() throws IOException {
                    super.close();
                    input.close();
                }
            };
            buffer.mark(1024);
            if (ArchiveUtils.isGzipped(buffer)) {
                log.debug("GZIP stream auto detected");
                buffer.reset();
                return new GZIPInputStream(buffer) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        buffer.close();
                    }
                };
            }
            buffer.reset();
            return buffer;
        }

        switch (hint) {
            case "": return input;
            case "gzip": return new GZIPInputStream(input) {
                @Override
                public void close() throws IOException {
                    super.close();
                    input.close();
                }
            };
            case "br": return new org.brotli.dec.BrotliInputStream(input) {
                @Override
                public void close() throws IOException {
                    super.close();
                    input.close();
                }
            };
            default: {
                log.warn("Unsupported compression hint '" + compressionHint + "'. Returning stream as-is");
                return input;
            }
        }
    }
}
