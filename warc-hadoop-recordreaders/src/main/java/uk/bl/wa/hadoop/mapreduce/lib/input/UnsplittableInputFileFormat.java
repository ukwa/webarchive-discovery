/**
 * 
 */
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class UnsplittableInputFileFormat
        extends FileInputFormat<Path, BytesWritable> {

    @Override
    public RecordReader<Path, BytesWritable> createRecordReader(InputSplit arg0,
            TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new ByteBlockRecordReader();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org.
     * apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

}
