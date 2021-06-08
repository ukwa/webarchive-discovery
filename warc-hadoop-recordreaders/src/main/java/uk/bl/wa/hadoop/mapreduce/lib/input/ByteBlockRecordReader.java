package uk.bl.wa.hadoop.mapreduce.lib.input;

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
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBlockRecordReader extends RecordReader<Path, BytesWritable> {
    private static final Logger log = LoggerFactory.getLogger(ByteBlockRecordReader.class);

    private FSDataInputStream fsdis;
    private Path path;
    private BytesWritable buf = new BytesWritable();
    private long bytes_read = 0;
    private long file_length = 0;
    private int buf_size = 1000 * 1000;

    @Override
    public void close() throws IOException {
        fsdis.close();
    }

    @Override
    public Path getCurrentKey() throws IOException, InterruptedException {
        return path;
    }

    @Override
    public BytesWritable getCurrentValue()
            throws IOException, InterruptedException {
        return buf;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bytes_read / ((float) file_length);
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (inputSplit instanceof FileSplit) {
            FileSplit fs = (FileSplit) inputSplit;
            path = fs.getPath();
            FileSystem fSys = path.getFileSystem(context.getConfiguration());
            fsdis = fSys.open(path);
            file_length = fSys.getContentSummary(path).getLength();
        } else {
            log.error("Only FileSplit supported!");
            throw new IOException("Need FileSplit input...");
        }

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        byte[] bytes = new byte[buf_size];
        // Attempt to read a chunk:
        int count = fsdis.read(bytes);
        // If we're out of bytes, report that:
        if (count == -1) {
            buf = null;
            return false;
        }
        bytes_read += count;
        // Otherwise, push the new bytes into the BytesWritable:
        buf = new BytesWritable(Arrays.copyOfRange(bytes, 0, count));
        return true;
    }

}
