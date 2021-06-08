/**
 * 
 */
package org.archive.io.warc;

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

import java.io.IOException;
import java.io.InputStream;

import org.archive.io.warc.WARCReader;

import com.google.common.io.CountingInputStream;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class UncompressedWARCReader extends WARCReader {

    public UncompressedWARCReader(final String f, final InputStream is,
            final boolean atFirstRecord)
        throws IOException {
            // Arc file has been tested for existence by time it has come
            // to here.
            setIn(new CountingInputStream(is));
            setCompressed(true);
            initialize(f);
        }
        

    public static WARCReader get(String arc, InputStream is, boolean atFirstRecord ) throws IOException {
        return new UncompressedWARCReader(arc, is, atFirstRecord);
    }

}
