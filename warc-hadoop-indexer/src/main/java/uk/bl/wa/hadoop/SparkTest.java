package uk.bl.wa.hadoop;

/*-
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2022 The webarchive-discovery project contributors
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

import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;

public class SparkTest {

    public static void main(String[] args) throws Exception {
        System.err.println("And first...");
        System.out.println("And first...");
        String appName = "test";
        String master = "local";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("And first...");

        JavaPairRDD<Text, WritableArchiveRecord> rdd = sc.hadoopFile("/Users/anj/Work/workspace/webarchive-discovery/temp/video_error.warc.gz", 
            ArchiveFileInputFormat.class, Text.class, WritableArchiveRecord.class);
        List<String> out = rdd.map(new Function<Tuple2<Text, WritableArchiveRecord>, String>() {

            @Override
            public String call(Tuple2<Text, WritableArchiveRecord> tuple) throws Exception {
                System.out.println(tuple._1());
                String url = tuple._2().getRecord().getHeader().getUrl();
                System.out.println(url);
                return url;
            }

        }).collect();

        System.out.println("And finally...");
        System.out.println(out);
        
        sc.close();
        
    }
    
}
