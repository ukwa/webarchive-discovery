/**
 * 
 */
package org.archive.io.arc;

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
