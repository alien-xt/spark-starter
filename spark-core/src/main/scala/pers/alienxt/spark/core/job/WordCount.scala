package pers.alienxt.spark.core.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：WordCount</p>
 * <p>内容摘要：单词统计</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午10:25
 */
object WordCount {

  def main(args: Array[String]) {
    // local model
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    if (masterUrl.contains("local")) {
      // 屏蔽日志
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    }

    // conf
    val conf = new SparkConf
    conf.setMaster("local[1]")
    conf.setAppName("UserClickCount")
    conf.set("SPARK_EXECUTOR_MEMORY", "2g")

    // init spark context
    val sc = new SparkContext(conf)

    // 整个目录
    val dirLines = sc.textFile("hdfs://localhost:9000/user/spark/input/conf/")
    // split word => map => reduce
    dirLines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .collect().foreach(wc => println("directory wc : " + wc))

    // 正则匹配
    val fileLines = sc.textFile("hdfs://localhost:9000/user/spark/input/conf/*.sh")
    // split word => map => reduce
    fileLines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .collect().foreach(wc => println("files wc : " + wc))

    // 压缩文件
    val compressedLines = sc.textFile("hdfs://localhost:9000/user/spark/input/compressed_file/spark_conf.tar")
    // split word => map => reduce
    compressedLines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .collect().foreach(wc => println("compressed file wc : " + wc))

    sc.stop()
  }

}
