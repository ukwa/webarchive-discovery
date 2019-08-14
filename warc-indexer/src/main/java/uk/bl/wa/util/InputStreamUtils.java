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

import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class InputStreamUtils {

  
  private static Log log = LogFactory.getLog(InputStreamUtils.class );
  
  /*
   * This method will detect if the inputstream is gzip and unzip the inputstream.
   * If inputstream is not zippet, the same inputstream will be returned. 
   *  
   */
  public static InputStream maybeDecompress(InputStream input) throws Exception {
    final PushbackInputStream pb = new PushbackInputStream(input, 2);

    int header = pb.read();
    if(header == -1) {
        return pb;
    }

    int b = pb.read();
    if(b == -1) {
        pb.unread(header);
        return pb;
    }

    pb.unread(new byte[]{(byte)header, (byte)b});

    header = (b << 8) | header;

    if(header == GZIPInputStream.GZIP_MAGIC) {
     log.debug("GZIP stream detected");  
      return new GZIPInputStream(pb);
    } else {
        return pb;
    }
 }
  
}
