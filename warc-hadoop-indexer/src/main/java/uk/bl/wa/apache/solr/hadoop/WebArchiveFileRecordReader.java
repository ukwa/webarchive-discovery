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
//import java.util.Iterator;
//
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.MultiFileSplit;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.log4j.Logger;
//import org.archive.io.ArchiveReader;
//import org.archive.io.ArchiveReaderFactory;
//import org.archive.io.ArchiveRecord;
//
//import uk.bl.wa.hadoop.WritableArchiveRecord;
//
///**
// * @author Andrew Jackson <Andrew.Jackson@bl.uk>
// *
// */
//public class WebArchiveFileRecordReader extends
//        RecordReader<Text, WritableArchiveRecord> {
//
//    private static Logger log = Logger
//            .getLogger(WebArchiveFileRecordReader.class.getName());
//
//    private FSDataInputStream datainputstream;
//    private FileStatus status;
//    private FileSystem filesystem;
//    private Path[] paths;
//    int currentPath = -1;
//    Long offset = 0L;
//    private ArchiveReader arcreader;
//    private Iterator<ArchiveRecord> iterator;
//    private ArchiveRecord record;
//    private String archiveName;
//    private WritableArchiveRecord currentRecord;
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
//     */
//    @Override
//    public void initialize(InputSplit split, TaskAttemptContext context)
//            throws IOException, InterruptedException {
//        if (split instanceof FileSplit) {
//            this.paths = new Path[1];
//            this.paths[0] = ((FileSplit) split).getPath();
//        } else if (split instanceof MultiFileSplit) {
//            this.paths = ((MultiFileSplit) split).getPaths();
//        } else {
//            throw new IOException(
//                    "InputSplit is not a file split or a multi-file split - aborting");
//        }
//        // get correct file system in case there are many (such as in EMR)
//        this.filesystem = FileSystem.get(this.paths[0].toUri(),
//                context.getConfiguration());
//        // Log the paths:
//        for (Path p : this.paths) {
//            log.info("Processing path: " + p);
//            System.out.println("Processing path: " + p);
//        }
//        // Queue up the iterator:
//        this.nextFile();
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
//     */
//    @Override
//    public boolean nextKeyValue() throws IOException, InterruptedException {
//        this.currentRecord = new WritableArchiveRecord();
//        return this.next(getCurrentKey(), currentRecord);
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
//     */
//    @Override
//    public Text getCurrentKey() throws IOException, InterruptedException {
//        return new Text();
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
//     */
//    @Override
//    public WritableArchiveRecord getCurrentValue() throws IOException,
//            InterruptedException {
//        return this.currentRecord;
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
//     */
//    @Override
//    public float getProgress() throws IOException, InterruptedException {
//        float progress = (float) datainputstream.getPos()
//                / (float) this.status.getLen();
//        return progress;
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.mapreduce.RecordReader#close()
//     */
//    @Override
//    public void close() throws IOException {
//        if (datainputstream != null) {
//            try {
//                datainputstream.close();
//            } catch (IOException e) {
//                log.error("close(): " + e.getMessage());
//            }
//        }
//    }
//
//    /**
//     * 
//     * @param key
//     * @param value
//     * @return
//     * @throws IOException
//     */
//    private boolean next(Text key, WritableArchiveRecord value)
//            throws IOException {
//        boolean found = false;
//        while (!found) {
//            boolean hasNext = false;
//            try {
//                hasNext = iterator.hasNext();
//            } catch (Throwable e) {
//                log.error("ERROR in hasNext():  " + this.archiveName + ": "
//                        + e.toString());
//                hasNext = false;
//            }
//            try {
//                if (hasNext) {
//                    record = (ArchiveRecord) iterator.next();
//                    found = true;
//                    key.set(this.archiveName);
//                    value.setRecord(record);
//                } else if (!this.nextFile()) {
//                    break;
//                }
//            } catch (Throwable e) {
//                found = false;
//                log.error("ERROR reading " + this.archiveName + ": "
//                        + e.toString());
//            }
//        }
//        return found;
//    }
//
//    private boolean nextFile() throws IOException {
//        currentPath++;
//        if (currentPath >= paths.length) {
//            return false;
//        }
//        // Output the archive filename, to help with debugging:
//        log.info("Opening nextFile: " + paths[currentPath]);
//        // Set up the ArchiveReader:
//        this.status = this.filesystem.getFileStatus(paths[currentPath]);
//        datainputstream = this.filesystem.open(paths[currentPath]);
//        arcreader = (ArchiveReader) ArchiveReaderFactory.get(
//                paths[currentPath].getName(), datainputstream, true);
//        // Set to strict reading, in order to cope with malformed archive files
//        // which cause an infinite loop otherwise.
//        arcreader.setStrict(true);
//        // Get the iterator:
//        iterator = arcreader.iterator();
//        this.archiveName = paths[currentPath].getName();
//        return true;
//    }
//
// }
