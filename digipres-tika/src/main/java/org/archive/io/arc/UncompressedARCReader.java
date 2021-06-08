/**
 * 
 */
package org.archive.io.arc;

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

import com.google.common.io.CountingInputStream;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class UncompressedARCReader extends ARCReader {

    public UncompressedARCReader(final String f, final InputStream is,
            final boolean atFirstRecord)
        throws IOException {
            // Arc file has been tested for existence by time it has come
            // to here.
            setIn(new CountingInputStream(is));
            setCompressed(true);
            setAlignedOnFirstRecord(atFirstRecord);
            initialize(f);
        }
        

    public static ARCReader get(String arc, InputStream is, boolean atFirstRecord ) throws IOException {
        return new UncompressedARCReader(arc, is, atFirstRecord);
    }

}
