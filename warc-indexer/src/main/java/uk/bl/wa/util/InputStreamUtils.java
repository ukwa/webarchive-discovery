package uk.bl.wa.util;

/*-
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2019 The webarchive-discovery project contributors
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
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.util.ArchiveUtils;


public class InputStreamUtils {
    private static Log log = LogFactory.getLog(InputStreamUtils.class );

    /*
     * Detects if the input stream is GZipped or Brotli-compressed. If so, the stream will be wrapped and automatically
     * decompressed upon use. If the input stream is not compressed, it will be returned wrapped in a BufferedInputStream.
     */
    public static InputStream maybeDecompress(InputStream input) throws Exception {
        final BufferedInputStream buf = new BufferedInputStream(input);
        buf.mark(1024);

        if (ArchiveUtils.isGzipped(buf)) {
            log.debug("GZIP stream detected");
            buf.reset();
            return new GZIPInputStream(buf);
        }

        // TODO: Add Brotli detection. Problem: It seems to be hard: https://stackoverflow.com/questions/39008957/is-there-a-way-to-check-if-a-buffer-is-in-brotli-compressed-format

        buf.reset();
        return buf;
    }
}
