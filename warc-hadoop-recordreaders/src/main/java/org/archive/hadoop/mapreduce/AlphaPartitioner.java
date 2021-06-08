package org.archive.hadoop.mapreduce;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Copied in from ia-hadoop-tools, which we should sync with.
 * 
 * 
 * TODO Depend on webarchive-hadoop-utils instead.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class AlphaPartitioner extends Partitioner<Text, Text> implements
        Configurable {
    // TODO: refactor
    private static final Charset UTF8 = Charset.forName("UTF-8");
    
    private static String CONFIG_SPLIT_PATH_NAME = "alphapartitioner.path";
    private String boundaries[] = new String[0];

    Configuration conf;

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String keyS = key.toString();
        int loc = Arrays.binarySearch(boundaries, keyS);
        if (loc < 0) {
            loc = (loc * -1) - 2;
            if (loc < 0) {
                loc = 0;
            }
        }
        return loc;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        String partitionPath = getPartitionPath(conf);
        String numReduceTasks = conf.get("mapred.reduce.tasks");
        System.err.println("AlphaPartitioner-reducers: " + numReduceTasks);
        System.err.println("AlphaPartitioner-partitionPath: " + partitionPath);
        try {
            URI uri = new URI(partitionPath);
            FileSystem fs = FileSystem.get(uri, conf);
            Path p = new Path(partitionPath);
            loadBoundaries(new BufferedReader(new InputStreamReader(fs.open(p))));
        } catch (IOException e) {
            // TODO: ugh. how to handle?
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param conf
     *            Configuration for the Job
     * @param path
     *            hdfs:// URI pointing to the split file
     */
    public static void setPartitionPath(Configuration conf, String path) {
        conf.set(CONFIG_SPLIT_PATH_NAME, path);
    }

    /**
     * @param conf
     *            Configuration for the Job
     * @return the hdfs:// URI for the split file configured for this job
     */
    public static String getPartitionPath(Configuration conf) {
        return conf.get(CONFIG_SPLIT_PATH_NAME);
    }
    
    public static int countLinesInPath(Path path, Configuration conf) 
    throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream is = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, UTF8));
        int lineCount = 0;
        while (br.readLine() != null) {
            lineCount++;
        }
        is.close();
        return lineCount;
    }

    private void loadBoundaries(BufferedReader bis) throws IOException {
        ArrayList<String> l = new ArrayList<String>();
        while (true) {
            String line = bis.readLine();
            if (line == null) {
                break;
            }
            l.add(line);
        }
        boundaries = l.toArray(boundaries);
        Arrays.sort(boundaries);
    }
}
