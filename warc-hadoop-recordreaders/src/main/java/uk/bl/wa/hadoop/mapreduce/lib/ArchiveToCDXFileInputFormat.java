package uk.bl.wa.hadoop.mapreduce.lib;

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
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ArchiveToCDXFileInputFormat extends FileInputFormat<Text, Text> {
    TextInputFormat input = null;

    @Override
    public List<InputSplit> getSplits( JobContext context ) throws IOException {
        if( input == null ) {
            input = new TextInputFormat();
        }
        return input.getSplits( context );
    }

    @Override
    public RecordReader<Text, Text> createRecordReader( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {
        return new DereferencingArchiveToCDXRecordReader<Text, Text>();
    }
}
