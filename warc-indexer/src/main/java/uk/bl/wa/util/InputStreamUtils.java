package uk.bl.wa.util;

/*-
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.util.ArchiveUtils;
import org.apache.commons.httpclient.ChunkedInputStream;

public class InputStreamUtils {
    private static Log log = LogFactory.getLog(InputStreamUtils.class );

    /**
     * Checks if an input stream seems to be chunked. If so, the stream content is de-chunked.
     * If not, the stream content is returned unmodified.
     * Chunked streams must begin with {@code ^[0-9a-z]{1,8}(;.{0,1024})?\r\n}.
     * @param input a stream with the response body from a HTTP-response.
     * @return the un-chunked content of the given stream.
     * @throws IOException if the stream could not be processed.
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding">Transfer-Encoding</a>
     */
    public static InputStream maybeDechunk(InputStream input) throws IOException {
        final BufferedInputStream buf = new BufferedInputStream(input);
        buf.mark(1024); // Room for a lot of comments
        int pos = 0;
        int c = -1;
        // Check for hex-number
        while (pos < 8) {
            c = buf.read();
            if (c== -1) { // EOF
                log.debug("maybeDechunk reached EOF while looking for hex digits at pos " + pos + ": " +
                          "Not a chunked stream, returning content as-is");
                buf.reset();
                return buf;
            }
            if (('0' <= c && c <= '9') || ('a' <= c && c <= 'f')) {
                pos++;
                continue;
            }
            break;
        }
        if (pos == 0 || pos == 8) {
            log.debug("maybeDechunk found " + pos + " hex digits: Not a chunked stream, returning content as-is");
            buf.reset();
            return buf;
        }
        // Check for \r\n or extension
        if (c == -1) { // EOF
            log.debug("maybeDechunk reached EOF while looking for extension or \\r\\n at pos " + pos + ": " +
                      "Not a chunked stream, returning content as-is");
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
                log.debug("maybeDechunk found hex digits and start of an extension but could not locate CRLF: " +
                          "Not a chunked stream, returning content as-is");
                buf.reset();
                return buf;
            }
            log.debug("maybeDechunk found hex digits and an extension: Probably chunked stream, returning content " +
                      "wrapped in a de-chunker");
            buf.reset();
            return new ChunkedInputStream(buf);
        }
        // Not with extension. Next chars must be CRLF
        if (c == '\r') { // CR
            c = buf.read();
            if (c == '\n') { // LF
                log.debug("maybeDechunk found hex digits CRLF: Probably chunked stream, returning content " +
                          "wrapped in a de-chunker");
                buf.reset();
                return new ChunkedInputStream(buf);
            }
        }
        log.debug("maybeDechunk found hex digits but could not locate CRLF: " +
                  "Not a chunked stream, returning content as-is");
        buf.reset();
        return buf;
    }

    /*
     * Provides a decompressing wrapper for the input. If the compressionHint is null, the method will attempt to
     * auto-guess if the input is GZip-compressed. Auto-guessing does not work for Brotli as such detection is
     * unreliable for that format.
     * If the hint is empty, the input stream will be returned unmodified.
     * @param input the input stream that might be decompressed.
     * @param compressionHint if present, this will be used for selecting the compression scheme.
     *       Usable values are 'GZip' and 'Br'. Not case-sensitive.
     */
    public static InputStream maybeDecompress(InputStream input, String compressionHint) throws IOException {
        final String hint = compressionHint == null ? null : compressionHint.toLowerCase().trim();
        if (hint == null) { // Auto-guess
            // Detecting Brotli is hard: https://stackoverflow.com/questions/39008957/is-there-a-way-to-check-if-a-buffer-is-in-brotli-compressed-format
            final BufferedInputStream buf = new BufferedInputStream(input);
            buf.mark(1024);
            if (ArchiveUtils.isGzipped(buf)) {
                log.debug("GZIP stream auto detected");
                buf.reset();
                return new GZIPInputStream(buf);
            }
            buf.reset();
            return buf;
        }
        switch (hint) {
            case "": return input;
            case "gzip": return new GZIPInputStream(input);
            case "br": return new org.brotli.dec.BrotliInputStream(input);
            default: {
                log.warn("Unsupported compression hint '" + compressionHint + "'. Returning stream as-is");
                return input;
            }
        }
    }
}
