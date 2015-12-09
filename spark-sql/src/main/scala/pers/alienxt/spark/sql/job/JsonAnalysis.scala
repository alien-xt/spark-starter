package pers.alienxt.spark.sql.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：JsonAnalysis</p>
 * <p>内容摘要： 由Json文件转DataFrames，查询，转换等操作</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/11/8 上午11:28
 */
object JsonAnalysis {

  val appName = "JsonAnalysis"

  case class Person(name: String, age: Int)

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
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 由json字串并行化成DataFrames
    val rdd1 = sc.parallelize(
      """{"name":"alien","age":"26","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val df1 = sqlContext.read.json(rdd1)
    df1.show()

    // 由json文件并行化成DataFrames
    val df2 = sqlContext.read.json("data/people.json")
    df2.show()
    df2.printSchema()
    df2.select(df2("name"), df2("age") + 1).show()
    df2.filter(df2("age") > 21).show()
    df2.groupBy("age").count().show()
    df2.select("name").show()
    // 将查询结果写到文件里
    df2.select("name").write.format("json").saveAsTable("result_1")


    // 由json文件转DataFrames
    val peoples = sc.textFile("data/people.log").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    // 注册表
    peoples.registerTempTable("people")

    // 执行sql
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 21")
    teenagers.map(p => "Name: " + p(0)).collect().foreach(println)
    teenagers.map(p => "Name: " + p.getAs[String]("name")).collect().foreach(println)
    teenagers.map(_.getValuesMap[Any](List("name"))).collect().foreach(println)
  }

}
