import jdk.nashorn.internal.runtime.arrays.IteratorAction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Michael on 11/5/15.
 */
public final class SelfJoin {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SelfJoin").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        java.sql.Timestamp startTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
        long startTime = System.nanoTime();
        JavaRDD<String> lines = ctx.textFile("file1", 1);

        //In case of a line with only one element we filter those lines out so that the mapToPair function will not crush
        JavaRDD<String> filteredLines = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String line = new String();
                String kMinusOne = new String();
                String kthItem = new String();
                int index;

                index = s.lastIndexOf(",");
                if (index == -1) {
                    return false;
                } else return true;
            }
        });

        JavaPairRDD<String, String> maps = filteredLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String line = new String();
                String kMinusOne = new String();
                String kthItem = new String();
                int index;

                index = s.lastIndexOf(",");
                if (index == -1) {
                    System.out.println("MapToPair: Input File in Wrong Format When Processing " + s);
                }
                kMinusOne = s.substring(0, index);
                kthItem = s.substring(index + 1);

                Tuple2<String, String> elem = new Tuple2<String, String>(kMinusOne, kthItem);
                return elem;
            }
        });

        long count = maps.count();
        //System.out.println(count);

        JavaPairRDD<String, Iterable<String>> groupedMap = maps.groupByKey();

        count = groupedMap.count();
        //System.out.println(count);

        JavaPairRDD<String, Iterable<String>> distinctValueMap = groupedMap.mapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> strings) throws Exception {
                List<String> kthItemList = new ArrayList<String>();
                for(String s: strings) {
                    if (!kthItemList.contains(s)) {
                        kthItemList.add(s);
                    }
                }
                Collections.sort(kthItemList);
                return kthItemList;
            }
        });

        count = distinctValueMap.count();
        //System.out.println(count);

        JavaPairRDD<String, String> result = distinctValueMap.flatMapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> strings) throws Exception {
                List<String> output = new ArrayList<String>();
                List<String> kthItemList = (List)strings;
                String outVal = new String ("");
                for (int i = 0; i < kthItemList.size() - 1; i++){
                    for (int j = i+1; j < kthItemList.size(); j++) {
                        outVal = kthItemList.get(i) + "," + kthItemList.get(j);
                        output.add(outVal);
                    }
                }
                return output;
            }
        });

        /*To check the result
        List<Tuple2<String, String>> output = result.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */

        ctx.stop();
    }
}
