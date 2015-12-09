package pers.alienxt.spark.graphx.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：PageRank</p>
 * <p>内容摘要：谷歌PageRank算法，计算出网页的排名</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 下午2:47
 */
object PageRank {

  val appName = "PageRank"

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

    // 读入数据文件
    val articles: RDD[String] = sc.textFile("data/graphx-wiki-vertices.txt")
    val links: RDD[String] = sc.textFile("data/graphx-wiki-edges.txt")

    // 装载顶点
    val vertices = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }

    // 装载边
    val edges = links.map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }

    // 构建图，并缓存  MEMORY_ONLY
    val graph = Graph(vertices, edges, "").persist()


    // pageRank算法里面的时候使用了cache()，故前面persist的时候只能使用MEMORY_ONLY
    val prGraph = graph.pageRank(0.001).cache()

    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    // 取排名top10
    titleAndPrGraph.vertices.top(10) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))

    sc.stop()
  }
}

