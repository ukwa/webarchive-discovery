package uk.bl.wa.spark;

import uk.bl.wa.Memento;

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


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;

/**
 * Run tests using Spark local mode, as per: https://spark.apache.org/docs/latest/rdd-programming-guide.html#unit-testing 
 */
public class WarcLoaderTest {

    public static void main(String[] args) throws Exception {
        String appName = "test";
        String master = "local";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        SparkSession spark = SparkSession
        .builder()
        .config(conf)
        .config("spark.sql.parser.quotedRegexColumnNames", true)
        //.config("spark.sql.parquet.compression.codec", "gzip") // SNAPPY hits annoying errors with ByteBuffers across J11/J8
        .appName("Java Spark SQL WARC example")
        .getOrCreate();

        Dataset<Row> df = WarcLoader.createDataFrame("/Users/anj/Work/workspace/webarchive-discovery/temp/video_error.warc.gz", spark);

        df.printSchema();

        //df.write().parquet("video_error.parquet");
        //df.printSchema();

        df.createOrReplaceTempView("mementos");

        //df.show();

        Dataset<Row> dft = spark.sql("SELECT url, `content.*` FROM mementos WHERE contentType IS NOT NULL");
        dft.show();


        spark.stop();

    }
    
}

