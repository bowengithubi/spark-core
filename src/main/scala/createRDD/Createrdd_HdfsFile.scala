package createRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author yxj
 * @DateTime 2021/12/30 0030 16:54
 */
object Createrdd_HdfsFile {
  def main(args: Array[String]): Unit = {
    // 1. 创建配置对象
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    // 2. 创建sc对象
    val sc = new SparkContext(conf)

    // 3. 使用sc进行编程

    val value: RDD[String] = sc.textFile("input", 2)
    value.collect().foreach(println)
    value.saveAsTextFile("rdd_hdfsfile_output")

    // 4. 关闭sc
    sc.stop()
  }
}
