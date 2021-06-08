/**
 * 
 */
package uk.bl.wa.hadoop.mapred;

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
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Streaming-API compatible version of the binary unsplittable file reader.
 * Implemented using the older API in order to be compatible with Hadoop
 * Streaming.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@SuppressWarnings("deprecation")
public class ByteBlockRecordReader
        implements RecordReader<Path, BytesWritable> {
    private static final Logger log = LoggerFactory.getLogger(ByteBlockRecordReader.class);

    private InputStream fsdis;
    private Path path;
    private long bytes_read = 0;
    private long file_length = 0;

    private CompressionCodecFactory compressionCodecs;

    /**
     * 
     * @param inputSplit
     * @param conf
     * @throws IOException
     */
    public ByteBlockRecordReader(InputSplit inputSplit, JobConf conf)
            throws IOException {
        if (inputSplit instanceof FileSplit) {
            FileSplit fs = (FileSplit) inputSplit;
            path = fs.getPath();
            FileSystem fSys = path.getFileSystem(conf);
            file_length = fSys.getContentSummary(path).getLength();
            fsdis = fSys.open(path);

            // Support auto-decompression of compressed files:
            boolean autoDecompress = conf.getBoolean(
                    "mapreduce.unsplittableinputfileformat.autodecompress",
                    false);
            if (autoDecompress) {
                log.warn("Enabling auto-decompression of this file.");
                compressionCodecs = new CompressionCodecFactory(conf);
                final CompressionCodec codec = compressionCodecs.getCodec(path);
                if (codec != null) {
                    fsdis = codec.createInputStream(fsdis);
                }
            } else {
                log.info("Auto-decompression is not enabled.");
            }
        } else {
            log.error("Only FileSplit supported!");
            throw new IOException("Need FileSplit input...");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        fsdis.close();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public Path createKey() {
        return path;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        return bytes_read;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        return bytes_read / ((float) file_length);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
     */
    @Override
    public boolean next(Path path, BytesWritable buf) throws IOException {
        int buf_size;
        long remaining = file_length - bytes_read;
        if (remaining < Integer.MAX_VALUE) {
            buf_size = (int) remaining;
        } else {
            buf_size = Integer.MAX_VALUE;
        }
        byte[] bytes = new byte[buf_size];

        // Attempt to read a big chunk (n.b. using a single .read() can require
        // multiple reads):
        int count = IOUtils.read(fsdis, bytes);

        // If we're out of bytes, report that:
        if (count == -1) {
            log.info("Read " + count + " bytes into RAM, total read: "
                    + bytes_read);
            buf.set(new byte[] {}, 0, 0);
            return false;
        } else {
            log.info("Read " + count + " bytes into RAM, total read: "
                    + bytes_read);
            bytes_read += count;
            // Otherwise, push the new bytes into the BytesWritable:
            buf.set(bytes, 0, count);
            return true;
        }
    }

}
