package pers.alienxt.spark.core.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：ArrayRddOperation</p>
 * <p>内容摘要：由Array类型并行化成rdd，对rdd进行转换及操作</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午11:28
 */
object ArrayRddOperation {

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

    // 并行化
    val num = sc.parallelize(1 to 10)
    val doubleNum = num.map(_ * 2)
    val threeNum = doubleNum.filter(_ % 3 == 0)
    threeNum.foreach(num => println("three num : " + num))

    // 并行化，并且指定task num
    val num1 = sc.parallelize(1 to 10, 4)
    val doubleNum1 = num1.map(_ * 2)
    val threeNum1 = doubleNum1.filter(_ % 3 == 0)
    threeNum1.foreach(num => println("three num1 : " + num))
    println("three num1 transform : " + threeNum1.toDebugString)

    // 缓存
    threeNum.cache()
    // 清除缓存
    threeNum.unpersist()

    // action
    num.take(5).foreach(num => println("take : " + num))
    println("count : " + num.count)
    val result = num.reduce(_ + _)
    println("reduce result : " + result)

    sc.stop()
  }

}
