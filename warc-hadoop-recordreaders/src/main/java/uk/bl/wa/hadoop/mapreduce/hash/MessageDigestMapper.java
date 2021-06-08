/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.hash;

/*
 * #%L
 * warc-hadoop-recordreaders
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MessageDigestMapper extends Mapper<Path, BytesWritable, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(MessageDigestMapper.class);

    public static final String CONFIG_DIGEST_ALGORITHM = "message.digest.algorithm";

    private Path current = null;
    private MessageDigest md;
    private long bytes_seen = 0;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void cleanup(
            Mapper<Path, BytesWritable, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        log.debug("Cleaning up and emitting final result...");
        super.cleanup(context);
        this.emit(context);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void setup(
            Mapper<Path, BytesWritable, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        super.setup(context);
        // Get the digest algorithm, defaulting to SHA-512:
        String algorithm = context.getConfiguration()
                .get(CONFIG_DIGEST_ALGORITHM, "SHA-512");
        //
        try {
            md = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(Path key, BytesWritable value,
            Mapper<Path, BytesWritable, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        if (!key.equals(current)) {
            // Extract and emit:
            this.emit(context);
            // Set up a new one:
            current = key;
            bytes_seen = 0;
            md.reset();
            log.info("Hashing " + current);
        }
        md.update(value.getBytes(), 0, value.getLength());
        bytes_seen += value.getLength();
    }

    private void emit(Context context) {
        if (current == null)
            return;
        // Otherwise:
        try {
            byte[] digest = md.digest(); // Get once as reset after .digest()
            Text name = new Text(current.getParent().toUri().getPath());
            Text hex = new Text(new String(Hex.encodeHex(digest)) + " "
                    + bytes_seen + " " + current.toUri().getPath());
            log.debug("Got " + name + " " + hex + " from " + bytes_seen);
            context.write(name, hex);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
