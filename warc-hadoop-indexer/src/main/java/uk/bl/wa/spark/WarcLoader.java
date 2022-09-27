package uk.bl.wa.spark;

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
import uk.bl.wa.Memento;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.util.Normalisation;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * 
 * Make a WebArchiveLoader.load that wraps the Hadoop stuff.
 * Convert from input to POJO: a Memento with:
 *  - Named fields for core properties, naming consistent with CDX etc.
 *  - The source file name and offset etc.
 *  - A @Transient reference to the underlying WritableArchiveRecord wrapped as HashCached etc.
 *  - A Hash<String,String> for arbitrary metadata extracted fields.
 * Possibly an 'enrich' convention, a bit like ArchiveSpark, with the WARC Indexer being wrapped to create an enriched Memento from a basic one.
 *  - e.g. rdd.mapPartitions(EnricherFunction) wrapped as...
 *  - JavaRDD<Memento> rdd = WebArchiveLoader.load("paths", JavaSparkContext).enrich(WarcIndexerEnricherFunction);
 * POJO allows mapping JavaRDD<Memento> to a Dataframe
 * Register dataframe as temp table df.createOrReplaceTempView("table")
 * Use Spark SQL + Iceberg to take the temp table and MERGE INTO a destination MegaTable.
 * (Partitioned by day? https://iceberg.apache.org/docs/latest/spark-ddl/#partitioned-by )
 * https://iceberg.apache.org/docs/latest/spark-writes/#merge-into
 */
public class WarcLoader {

    /**
     * 
     * @param path
     * @param sc
     * @return
     */
    public static JavaPairRDD<Text, WritableArchiveRecord> load(String path, JavaSparkContext sc) {
        JavaPairRDD<Text, WritableArchiveRecord> rdd = sc.hadoopFile(path, 
            ArchiveFileInputFormat.class, Text.class, WritableArchiveRecord.class);
        return rdd;
    }
    
    /**
     * 
     * @param path
     * @param sc
     * @return
     */
    public static JavaRDD<Memento> loadAndAnalyse(String path, JavaSparkContext sc) {
        JavaPairRDD<Text, WritableArchiveRecord> rdd = WarcLoader.load(path, sc);
        JavaRDD<Memento> mementosRDD = rdd.mapPartitions(new WarcLoader.WarcIndexMapFunction(sc));
        return mementosRDD;
    }

    /**
     * 
     * @param path
     * @param spark
     * @return
     */
    public static Dataset<Row> createDataFrame(String path, SparkSession spark) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> df = spark.createDataFrame(WarcLoader.loadAndAnalyse(path, sc), Memento.class);
        return df;
    }

    public static class WarcIndexMapFunction implements FlatMapFunction<Iterator<Tuple2<Text, WritableArchiveRecord>>, Memento> {

        private Broadcast<Config> broadcastIndexConfig;
        private LongAccumulator recordsCounter;
        private LongAccumulator extractedRecordsCounter;

        public WarcIndexMapFunction(JavaSparkContext sc) {
            Config indexConfig = ConfigFactory.load();
            broadcastIndexConfig = sc.broadcast(indexConfig);
            recordsCounter = sc.sc().longAccumulator("warc_records_processed");
            extractedRecordsCounter = sc.sc().longAccumulator("warc_records_extracted");
        }

        @Override
        public Iterator<Memento> call(Iterator<Tuple2<Text, WritableArchiveRecord>> t) throws Exception {
            WARCIndexer index = new WARCIndexer( broadcastIndexConfig.getValue() );

            List<Memento> output = new ArrayList<Memento>();
            while( t.hasNext() ) {
                Tuple2<Text, WritableArchiveRecord> tuple = t.next();
                ArchiveRecordHeader header = tuple._2.getRecord().getHeader();
                ArchiveRecord rec = tuple._2.getRecord();
                // Create a minimal record:
                Memento minimal = new Memento();
                minimal.setId( "source:" + tuple._1 + "@" + header.getOffset());
                minimal.setSourceFile(tuple._1.toString());
                minimal.setSourceFileOffset(header.getOffset());
                minimal.setUrl(Normalisation.sanitiseWARCHeaderValue(header.getUrl()));
                minimal.setUrlType(SolrFields.SOLR_URL_TYPE_UNKNOWN);
                //minimal.setRecordType(rec);
        
                if (!header.getHeaderFields().isEmpty()) {
                    if( header.getHeaderFieldKeys().contains( WARCConstants.HEADER_KEY_TYPE ) ) {
                        minimal.setRecordType((String)header.getHeaderFields().get(WARCConstants.HEADER_KEY_TYPE));
                    }
                    // Do the indexing:
                    SolrRecord solr = index.extract(tuple._1.toString(),rec);
                    if( solr != null) {
                        output.add(solr.toMemento());
                        // Counter
                        extractedRecordsCounter.add(1);
                    } else {
                        output.add(minimal);
                    }
                }
                // Counter
                recordsCounter.add(1);

            }
            System.out.println("Processed "+ recordsCounter.value() + " record(s)...");
            System.out.println("Extracted "+ extractedRecordsCounter.value() + " record(s)...");
            return output.iterator();
        }

    }
    
}
