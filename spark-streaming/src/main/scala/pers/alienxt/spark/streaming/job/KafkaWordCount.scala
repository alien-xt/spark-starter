package pers.alienxt.spark.streaming.job

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <p>文件描述：KafkaWordCount</p>
 * <p>内容摘要： 从Kafka读取消息，进行wordcount，结果保存在hdfs里</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午10:12
 */
object KafkaWordCount {

  val appName = "KafkaWordCount"

  /**
   * 统计
   * @param newValues 新的统计值
   * @param runningCount 已经统计的值
   * @return 当前统计结果
   */
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum
    val oldCount = runningCount.getOrElse(0)
    Some(newCount + oldCount)
  }

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

    // init streaming context 每5秒作为一个批次
    val ssc = new StreamingContext(conf, Seconds(5))
    // 设置检查点 使用updateFunction必须设置
    ssc.checkpoint("hdfs://localhost:9000/user/spark/data/streaming/wordcount/checkpoint/")

    // 设置kafka信息
    val topics = Set("t_word_count")
    val brokers = "localhost:9092"
    val groupId = "g_t_word_count"
    val kafkaParams = Map[String, String] (
      "group.id" -> groupId,
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    // 创建DStream
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    // 切词，统计
    val words = kafkaDStream.flatMap(_._2.split(" "))
    val pairs = words.map((_, 1))
    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)
    // 输出
    runningCounts.print()
    // 保存到hdfs
    runningCounts.saveAsTextFiles("hdfs://localhost:9000/user/spark/output/streaming/wordcount/", "")

    ssc.start()
    ssc.awaitTermination()
  }

}
