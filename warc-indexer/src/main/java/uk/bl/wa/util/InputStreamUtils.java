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


public class InputStreamUtils {
    private static Log log = LogFactory.getLog(InputStreamUtils.class );

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
