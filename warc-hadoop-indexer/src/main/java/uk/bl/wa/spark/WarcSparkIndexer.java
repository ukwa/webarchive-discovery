package uk.bl.wa.spark;

/*-
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2023 The webarchive-discovery project contributors
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

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import uk.bl.wa.Memento;
import uk.bl.wa.MementoRecord;
import uk.bl.wa.hadoop.WritableArchiveRecord;

public class WarcSparkIndexer {
    

    public static void main(String[] args) throws Exception {
        String appName = "WarcSparkIndexer";
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Text, WritableArchiveRecord> rdd = WarcLoader.load(args[0], sc);
        List<MementoRecord> out = rdd.mapPartitions(new WarcLoader.WarcIndexMapFunction(sc)).collect();

        for (MementoRecord m : out) {
            System.out.println(m.getSourceFilePath());
        }
        sc.close();
        
    }
    
}


/*
 *  
 */
