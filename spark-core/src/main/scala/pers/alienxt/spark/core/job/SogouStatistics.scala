package pers.alienxt.spark.core.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：SogouStatistics</p>
 * <p>内容摘要：Sogou日志统计
 *            1.统计出排名为1，用户点击次序为2
 *            2.统计出用户查询次数最多的top</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 下午2:47
 */
object SogouStatistics {

  /**
   * sogou搜索排行文件格式
   * 访问时间 用户ID  [查询词] 该URL在返回结果中的排名 用户点击的顺序号  用户点击的URL
   * 20111230000009	698956eb07815439fe5f46e9a4503997	youku	1	1	http://www.youku.com/
   */


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
    val rdd1 = sc.textFile("hdfs://localhost:9000/user/spark/input/sogou/SogouQ.mini")
    println(" total line : " + rdd1.count())

    // 过滤清洗异常日志
    val rdd2 = rdd1.map(_.split("\t")).filter(_.length == 6)
    println(" structuring line : " + rdd2.count())

    // 统计出搜索排名第2，用户点击次序第2的条数
    val rdd3 = rdd2.filter(_(3).toInt == 1).filter(_(4).toInt == 2)
    println(" result : " + rdd3.count())

    // 每个用户查询的次数
    val rdd4 = rdd2.map(x => (x(1), 1)).reduceByKey(_ + _)
    // 按照用户查询次数降序
    val rdd5 = rdd4.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    // 取出用户查询次数最多的 top 10
    rdd5.take(10).foreach(x => println("user : " + x._1, " count : " + x._2))

    sc.stop()
  }

}
