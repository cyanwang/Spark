package Spark.a1;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.reflect.internal.Trees.Return;

 

public class wordcount {

	public static void main(String[]args)
	{
		//spark得配置信息
		SparkConf conf=new SparkConf()
				.setAppName("wordcount")
				.setMaster("local");
		
		//sparkcontext所有功能得入口，编写不同类型得spark用得context类型不一样
		JavaSparkContext  sc=new  JavaSparkContext(conf);
		
		
		//根据输入源(hdfs,本地文件)创建一个初始得RDD
		//数据源会被打散到partition中		
		JavaRDD<String> lines=sc.textFile("D:/haha.txt");
		
		//对初始RDD计算
		JavaRDD <String>words=lines.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return  Arrays.asList(line.split(" ")).iterator(); 
			}
		});
 	 
 
		//单词映射成（word,1）格式
		JavaPairRDD<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {//1输入类型，23是tuple得类型

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
			
		});
		//统计redeucebykey，对每个key对应得vlaue做reduce操作
		JavaPairRDD<String,Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {//12输入，3输出
			
			public Integer call(Integer v1, Integer v2) throws Exception {
			  return v1+v2;
			}
		});
		
		//控制台输出，action操作
		wordcounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			public void call(Tuple2<String, Integer> wordcount) throws Exception {
				System.out.println(wordcount._1+"   appeared:  "+wordcount._2);
				
			}
		});
		
	}
}
































