/**
 * 
 */
package org.archive.io.warc;

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
