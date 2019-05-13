import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//使用Java语言开发Spark任务：WordCount
/*
 * 数据: hdfs://hadoop111:9000/data/data.txt
 * I love Beijing
 * I love China
 * Beijing is the capital of China
 */
public class JavaWordCount {

    //定义模式：使用空格进行分词
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        //创建SparkConf
        SparkConf conf = new SparkConf().setAppName("My Java WordCount");

        //创建 SparkContext对象: JavaSparkContext
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //从HDFS上读取数据
        JavaRDD<String>  lines = ctx.textFile(args[0]);

        //压平操作，并且分词: 返回每一个单词
        JavaRDD<String>  words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                // s: 代表读入的数据: I love Beijing
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        //对每一个单词生产一个元组：(I,1)
        //k2和v2就是Map输出的结果
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                // s代表单词： Beijing ----> (Beijing,1)
                return new Tuple2<String, Integer>(s,1);
            }
        });

        //reduce处理：reduceByKey(_+_)
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                //把i1和i2累积求和
                return i1+i2;
            }
        });

        //触发计算得到最后的结果：调用action算子
        List<Tuple2<String, Integer>>  result = counts.collect();

        //输出结果: 直接打印在屏幕上
        for(Tuple2<String, Integer> t:result){
            System.out.println(t._1()+":"+t._2());
        }

        //停止context对象
        ctx.stop();
    }
}

