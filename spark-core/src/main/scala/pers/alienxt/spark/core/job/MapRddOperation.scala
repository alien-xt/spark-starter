package pers.alienxt.spark.core.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：MapRddOperation</p>
 * <p>内容摘要：由Map类型并行化成rdd，对rdd进行转换及操作</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午11:55
 */
object MapRddOperation {

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
    val kv1 = sc.parallelize(List(("A", 1), ("B", 2), ("C", 3), ("A", 10)))

    // action

    // 排序
    kv1.sortByKey().collect().foreach(kv => println("sort by key : " + kv))
    // 按key分组
    kv1.groupByKey().collect().foreach(kv => println("group by key : " + kv))
    // 按key统计
    kv1.reduceByKey(_ + _).collect().foreach(kv => println("reduce by key : " + kv))

    val kv2 = sc.parallelize(List(("A", 100), ("A", 100)))
    // 去重
    kv2.distinct().collect().foreach(kv => println("distinct : " + kv))
    // 合并
    kv1.union(kv2).collect().foreach(kv => println("union : " + kv))
    // join
    kv1.join(kv2).collect().foreach(kv => println("join : " + kv))

    sc.stop()
  }

}
