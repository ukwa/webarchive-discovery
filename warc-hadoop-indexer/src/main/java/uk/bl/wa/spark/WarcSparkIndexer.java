package uk.bl.wa.spark;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import uk.bl.wa.hadoop.WritableArchiveRecord;

public class WarcSparkIndexer {
    

    public static void main(String[] args) throws Exception {
        String appName = "WarcSparkIndexer";
        String master = "local[2]";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Text, WritableArchiveRecord> rdd = WarcLoader.load(args[0], sc);
        List<String> out = rdd.mapPartitions(new WarcLoader.WarcIndexMapFunction(sc)).collect();

        System.out.println(out);
        
        sc.close();
        
    }
    
}
