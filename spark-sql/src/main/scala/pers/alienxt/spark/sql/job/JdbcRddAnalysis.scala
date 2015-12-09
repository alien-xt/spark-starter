package pers.alienxt.spark.sql.job

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：JdbcRddAnalysis</p>
 * <p>内容摘要： 查询Mysql的数据，转成DataFrames</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/11/8 下午5:26
 */
object JdbcRddAnalysis {

  val appName = "JdbcRddAnalysis"

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
    conf.setAppName(appName)
    conf.set("SPARK_EXECUTOR_MEMORY", "2g")

    // init spark context
    val sc = new SparkContext("local[1]", "mysql")

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "")
      },
      "select * from test where id > ? and id < ?",
      0, 100, 3,
      r => r.getInt(1)).cache()

    rdd.foreach(id => println(" id : " + id))
    sc.stop()
  }

}
