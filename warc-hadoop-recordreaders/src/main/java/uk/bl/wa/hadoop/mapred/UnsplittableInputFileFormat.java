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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * Implemented using the older API in order to be compatible with Hadoop
 * Streaming.
 * 
 * Note that to use multiple records in streaming mode (as we do in the
 * uk.bl.wa.hadoop.mapreduce.lib.input.UnsplittableInputFileFormat) the streamed
 * process would have to cope with newlines coming up in the middle of the byte
 * streams. This could only work if we split on GZip record boundaries, which
 * would require a more complex implementation (like
 * https://github.com/helgeho/HadoopConcatGz but using the old API).
 * 
 * Hence, this version just tries to read the whole file into RAM.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@SuppressWarnings("deprecation")
public class UnsplittableInputFileFormat
        extends FileInputFormat<Path, BytesWritable> {

    @Override
    public RecordReader<Path, BytesWritable> getRecordReader(InputSplit inputSplit,
            JobConf conf, Reporter reporter) throws IOException {
        return new ByteBlockRecordReader(inputSplit, conf);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.FileInputFormat#isSplitable()
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

}
