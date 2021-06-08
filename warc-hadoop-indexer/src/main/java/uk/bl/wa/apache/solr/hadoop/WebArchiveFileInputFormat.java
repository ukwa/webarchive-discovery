package uk.bl.wa.apache.solr.hadoop;

/*
 * #%L
 * warc-hadoop-indexer
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
///**
// * 
// */
//package org.apache.solr.hadoop;
//
//import java.io.IOException;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//
//import uk.bl.wa.hadoop.WritableArchiveRecord;
//
///**
// * @author Andrew Jackson <Andrew.Jackson@bl.uk>
// *
// */
//public class WebArchiveFileInputFormat extends
//        FileInputFormat<Text, WritableArchiveRecord> {
//
//    @Override
//    public RecordReader<Text, WritableArchiveRecord> createRecordReader(
//            InputSplit split, TaskAttemptContext context) throws IOException,
//            InterruptedException {
//        WebArchiveFileRecordReader rr = new WebArchiveFileRecordReader();
//        rr.initialize(split, context);
//        return rr;
//
//    }
//
//    /*
//     * (non-Javadoc)
//     * 
//     * @see
//     * org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org
//     * .apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
//     */
//    @Override
//    protected boolean isSplitable(JobContext context, Path filename) {
//        return false;
//    }
//
// }
