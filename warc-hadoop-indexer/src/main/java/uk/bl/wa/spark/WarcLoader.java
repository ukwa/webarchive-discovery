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

import scala.Tuple2;
import uk.bl.wa.Memento;
import uk.bl.wa.MementoRecord;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.WritableArchiveRecord;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.util.Normalisation;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import org.apache.hadoop.io.Text;
import org.apache.james.mime4j.dom.field.FieldName;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.apache.solr.common.util.Utils;

/**
 * 
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
    public static JavaRDD<MementoRecord> loadAndAnalyse(String path, JavaSparkContext sc) {
        JavaPairRDD<Text, WritableArchiveRecord> rdd = WarcLoader.load(path, sc);
        JavaRDD<MementoRecord> mementosRDD = rdd.mapPartitions(new WarcLoader.WarcIndexMapFunction(sc));
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
        //Dataset<Row> df = spark.createDataFrame(WarcLoader.loadAndAnalyse(path, sc), Memento.class);
        JavaRDD<MementoRecord> mementos = WarcLoader.loadAndAnalyse(path, sc);
        JavaRDD<Row> mementoRows = mementos.map((Function<MementoRecord, Row>) record -> {
            return WarcLoader.getRow(record);
          });
        Dataset<Row> df = spark.createDataFrame(mementoRows, WarcLoader.getSchema());
        return df;
    }

    public static class WarcIndexMapFunction implements FlatMapFunction<Iterator<Tuple2<Text, WritableArchiveRecord>>, MementoRecord> {

        private Broadcast<Config> broadcastIndexConfig;
        private LongAccumulator recordsCounter;
        private LongAccumulator extractedRecordsCounter;

        public WarcIndexMapFunction(JavaSparkContext sc) {
            Config indexConfig = ConfigFactory.load("spark");
            broadcastIndexConfig = sc.broadcast(indexConfig);
            recordsCounter = sc.sc().longAccumulator("warc_records_processed");
            extractedRecordsCounter = sc.sc().longAccumulator("warc_records_extracted");
        }

        @Override
        public Iterator<MementoRecord> call(Iterator<Tuple2<Text, WritableArchiveRecord>> t) throws Exception {
            WARCIndexer index = new WARCIndexer( broadcastIndexConfig.getValue() );

            List<MementoRecord> output = new ArrayList<MementoRecord>();
            while( t.hasNext() ) {
                Tuple2<Text, WritableArchiveRecord> tuple = t.next();
                ArchiveRecordHeader header = tuple._2.getRecord().getHeader();
                ArchiveRecord rec = tuple._2.getRecord();
                // Create a minimal fallback record:
                // See also WARCIndexer.processEnvelopeHeader
                SolrRecord sr = SolrRecordFactory.DEFAULT_FACTORY.createRecord(tuple._1.toString() , header);
        
                // Do the indexing:
                SolrRecord solr = index.extract(tuple._1.toString(),rec);
                if( solr != null) {
                    sr = solr;
                    // Counter
                    extractedRecordsCounter.add(1);
                }


                // Experimenting with JSON
                ObjectMapper mapper = new ObjectMapper();
                //mapper.setVisibility(PropertyAccessor.FIELD, Visibility.NONE);
                String jsonInString = mapper.writeValueAsString(sr.toMemento());      
                System.out.println( "JSON: " + jsonInString ); 

                // And add converted form:
                MementoRecord mr = sr.toMementoRecord();
                output.add(mr);

                // Counter
                recordsCounter.add(1);

            }
            System.out.println("Processed "+ recordsCounter.value() + " record(s)...");
            System.out.println("Extracted "+ extractedRecordsCounter.value() + " record(s)...");
            return output.iterator();
        }

    }

    public static StructType getSchema() {
        SolrRecord empty = SolrRecordFactory.DEFAULT_FACTORY.createRecord();
        empty.setField(SolrFields.SOURCE_FILE_OFFSET, "0");
        return getSchema(empty.toMementoRecord());
    }
 
    public static Row getRow(MementoRecord mr) {
        // Goes through fields in a deterministic order to build up a Row:
        List<Object> ml = new ArrayList<Object>();
        ml.add(mr.getSourceFilePath());
        ml.add(mr.getSourceFileOffset());
        SortedMap<String,Object> md = mr.getMetadata();
        for (String fieldName : md.keySet()) {
            ml.add(md.get(fieldName));
        }   
        Row row = RowFactory.create(ml.toArray());
        return row;
    }

    public static StructType getSchema(MementoRecord mr) {
        // Goes through fields in deterministic over to build up the Schema:
        List<StructField> fields = new ArrayList<>();
        // Base
        fields.add(DataTypes.createStructField("source_file_path", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("source_file_offset", DataTypes.LongType, true));
        // Metadata
        SortedMap<String,Object> md = mr.getMetadata();
        SortedMap<String,Class> mt = mr.getMetadataTypes();
        for (String fieldName : md.keySet()) {
            Object o = md.get(fieldName);
            Class c = mt.get(fieldName);
            StructField field;
            // Encode based on class:
            if( c.equals(String.class) ) {
                field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            } else {
                throw new RuntimeException("Could not generate schema for field " + fieldName + ": "+ o);
            }
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

}
