package pers.alienxt.spark.graphx.job

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>文件描述：SimpleGraphX</p>
 * <p>内容摘要：图计算</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 下午2:47
 */
object SimpleGraphX {

  val appName = "SimpleGraphX"

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

    // 定义顶点数组
    val vertexArray = Array(
      (1L, ("Alien", 25)),
      (2L, ("Brian", 27)),
      (3L, ("Chris", 56)),
      (4L, ("David", 43)),
      (5L, ("Eric", 56)),
      (6L, ("Frank", 52))
    )

    // 定义边数组
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // 构造图Graph[VDD,EDD]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    println("************************属性操作************************")
    // 图中年龄大于30的顶点
    println("找出图中年龄大于30的顶点：")
    graph.vertices.filter(v => v._2._2 > 30).collect.foreach(
      v => println(s"${v._2._1} is ${v._2._2}")
    )
    println

    // 图中属性大于5的边
    println("找出图中属性大于5的边：")
    graph.edges.filter(e => e.attr > 5).collect.foreach(
      e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}")
    )
    println


    // triplets(三元组)，包含源点、源点属性、目标点、目标点属性、边属性
    println("所有的tripltes：")
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    println("边属性>5的tripltes：")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    // 度操作
    println("找出图中最大的出度、入度、度数：")
    def maxDegree(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(maxDegree) + " max of inDegrees:"
      + graph.inDegrees.reduce(maxDegree) + " max of Degrees:" + graph.degrees.reduce(maxDegree))
    println

    println("************************转换操作************************")
    println("顶点的转换操作，顶点age + 1：")
    graph.mapVertices { case (id, (name, age)) => (id, (name, age + 1))}
      .vertices.collect.foreach(
        v => println(s"${v._2._1} is ${v._2._2}")
      )
    println

    println("边的转换操作，边的属性*3：")
    graph.mapEdges(e => e.attr * 3).edges.collect.foreach(
      e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}")
    )
    println


    println("************************结构操作************************")
    println("顶点年纪>30的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println
    println("子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println


    println("************************连接操作************************")
    // 所有入度
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    // 创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0)}

    // initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }

    println("连接图的属性：")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
    println

    println("出度和入读相同的人员：")
    userGraph.vertices.filter {
      case (id, u) => u.inDeg == u.outDeg
    }.collect.foreach {
      case (id, property) => println(property.name)
    }
    println


    println("************************聚合操作************************")
    println("找出年纪最大的追求者：")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.mapReduceTriplets[(String, Int)](
      // 将源顶点的属性发送给目标顶点，map过程
      edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
      // 得到最大追求者，reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )

    userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str)}
    println

    // 找出追求者的平均年纪
    println("找出追求者的平均年纪：")
    val averageAge: VertexRDD[Double] = userGraph.mapReduceTriplets[(Int, Double)](
      // 将源顶点的属性 (1, Age)发送给目标顶点，map过程
      edge => Iterator((edge.dstId, (1, edge.srcAttr.age.toDouble))),
      // 得到追求着的数量和总年龄
      (a, b) => ((a._1 + b._1), (a._2 + b._2))
    ).mapValues((id, p) => p._2 / p._1)

    userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) =>
      optAverageAge match {
        case None => s"${user.name} does not have any followers."
        case Some(avgAge) => s"The average age of ${user.name}\'s followers is $avgAge."
      }
    }.collect.foreach { case (id, str) => println(str)}
    println

    sc.stop()
  }
}

