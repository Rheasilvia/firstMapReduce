import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class SecondarySort {

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: SecondarySort<file>");
//            System.exit(1);
//        }

        String inputPath = "/home/mj/Documents/firstMapReduce/src/main/resources/test";
//        System.out.println("args[0: <file>=" + args[0]);

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        //从javaRDD创建键值对
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair((PairFunction<String, String, Tuple2<Integer, Integer>>) s -> {
            String[] tokens = s.split(",");
            System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
            Integer time = new Integer(tokens[1]);
            Integer value = new Integer(tokens[2]);
            Tuple2<Integer, Integer> timeValue = new Tuple2<>(time, value);
            return new Tuple2<>(tokens[0], timeValue);
        });

        //验证
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();

        output.stream().forEach(t -> {
            Tuple2<Integer, Integer> timeValue = t._2;
            System.out.println(t._1 + "," + timeValue._1 + "," + timeValue._1);
        });

        //分组
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();

        //验证
        System.out.println("======DEBUGE======");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();

        output2.stream().forEach(t -> {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            list.forEach(t2 -> {
                System.out.println(t2._1 + "," + t2._2);
            });

            System.out.println("===========");
        });
    }
}
