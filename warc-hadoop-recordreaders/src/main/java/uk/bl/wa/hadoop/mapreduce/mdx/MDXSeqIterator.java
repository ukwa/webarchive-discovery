package uk.bl.wa.hadoop.mapreduce.mdx;

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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.json.JSONException;

/**
 * This helper class can iterate through a sequence file that holds MDX records.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDXSeqIterator implements Iterator<MDX> {

    private Reader reader;
    private Text key;
    private Text value;
    private boolean hasNext;

    public MDXSeqIterator(File seq) throws IOException,
            InstantiationException, IllegalAccessException {
        Configuration config = new Configuration();
        Path path = new Path(seq.getAbsolutePath());
        reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
        key = (Text) reader.getKeyClass().newInstance();
        value = (Text) reader.getValueClass().newInstance();
        // Queue up:
        hasNext = reader.next(key, value);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public MDX next() {
        MDX mdx;
        try {
            mdx = new MDX(value.toString());
        } catch (JSONException e1) {
            mdx = null;
        }
        try {
            hasNext = reader.next(key, value);
        } catch (IOException e) {
            hasNext = false;
        }
        return mdx;
    }

    @Override
    public void remove() {
    }

}
