/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce;

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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.bl.wa.hadoop.WritableArchiveRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WebArchiveRecordReader
        extends RecordReader<Text, WritableArchiveRecord> {

    private static Logger log = LoggerFactory
            .getLogger(WebArchiveRecordReader.class.getName());

    private FSDataInputStream datainputstream;
    private FileStatus status;
    private FileSystem filesystem;
    private Path[] paths;
    int currentPath = -1;
    Long offset = 0L;
    private ArchiveReader arcreader;
    private Iterator<ArchiveRecord> iterator;
    private ArchiveRecord record;
    private String archiveName;

    private Text key = new Text();
    private WritableArchiveRecord value = new WritableArchiveRecord();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (split instanceof FileSplit) {
            this.paths = new Path[1];
            this.paths[0] = ((FileSplit) split).getPath();
        } else {
            throw new IOException(
                    "InputSplit is not a file split or a multi-file split - aborting");
        }
        // get correct file system in case there are many (such as in EMR)
        this.filesystem = FileSystem.get(this.paths[0].toUri(),
                context.getConfiguration());

        // Log the paths and check for empty files:
        List<Path> validPaths = new ArrayList<Path>();
        for (Path p : this.paths) {
            log.info("Processing path: " + p);
            FileStatus s = this.filesystem.getFileStatus(p);
            if (s.getLen() == 0) {
                log.warn("Skipping empty file: " + p);
            } else {
                validPaths.add(p);
            }
        }
        // Use this list instead:
        this.paths = validPaths.toArray(new Path[0]);

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (datainputstream != null && this.status != null) {
            return (float) datainputstream.getPos()
                    / (float) this.status.getLen();
        } else {
            return 1.0f;
        }
    }

    @Override
    public void close() throws IOException {
        if (datainputstream != null) {
            try {
                datainputstream.close();
            } catch (IOException e) {
                log.error("close(): " + e.getMessage());
            }
        }
    }

    @Override
    public Text getCurrentKey()
            throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public WritableArchiveRecord getCurrentValue()
            throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean found = false;
        while (!found) {
            boolean hasNext = false;
            try {
                hasNext = iterator.hasNext();
            } catch (Throwable e) {
                log.error("ERROR in hasNext():  " + this.archiveName + ": "
                        + e.toString());
                hasNext = false;
            }
            try {
                if (hasNext) {
                    record = (ArchiveRecord) iterator.next();
                    found = true;
                    this.key.set(this.archiveName);
                    this.value.setRecord(record);
                } else if (!this.nextFile()) {
                    break;
                }
            } catch (Throwable e) {
                found = false;
                log.error("ERROR reading " + this.archiveName, e);
                // Reached the end of the file? If so move on or exit:
                if (e.getCause() instanceof EOFException) {
                    log.error("EOF while reading " + this.archiveName);
                    if (!this.nextFile()) {
                        break;
                    }
                }
            }
        }
        return found;
    }

    private boolean nextFile() throws IOException {
        currentPath++;
        if (currentPath >= paths.length) {
            return false;
        }
        // Output the archive filename, to help with debugging:
        log.info("Opening nextFile: " + paths[currentPath]);
        // Set up the ArchiveReader:
        this.status = this.filesystem.getFileStatus(paths[currentPath]);
        datainputstream = this.filesystem.open(paths[currentPath]);
        arcreader = (ArchiveReader) ArchiveReaderFactory
                .get(paths[currentPath].getName(), datainputstream, true);
        // Set to strict reading, in order to cope with malformed archive files
        // which cause an infinite loop otherwise.
        arcreader.setStrict(true);
        // Get the iterator:
        iterator = arcreader.iterator();
        this.archiveName = paths[currentPath].getName();
        return true;
    }

}
