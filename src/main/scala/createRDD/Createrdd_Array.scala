package createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.lang.Thread.sleep

/**
 * @Author yxj
 * @DateTime 2021/12/29 0029 20:06
 */

object Createrdd_Array {
  def main(args: Array[String]): Unit = {
    //    1.创建配置对象
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    //   2.创建sc对象
    val sc = new SparkContext(conf)
    //   3.使用sc进行编程
    //val value:RDD[Int] = sc.parallelize(List(6, 2, 3, 4, 5))
    val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    value.saveAsTextFile("rdd_array_output")
    value.collect().foreach(println)
    sleep(100)
    //   4.关闭sc
    sc.stop()
  }


}
