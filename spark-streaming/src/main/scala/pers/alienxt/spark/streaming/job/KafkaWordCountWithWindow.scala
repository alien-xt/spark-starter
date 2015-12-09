package pers.alienxt.spark.streaming.job

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <p>文件描述：KafkaWordCountWithWindow</p>
 * <p>内容摘要： 从Kafka读取消息，进行wordcount，每隔一段时间统计之前一段时间的数据</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午10:12
 */
object KafkaWordCountWithWindow {

  val appName = "KafkaWordCountWithWindow"

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

    // 切词
    val words = kafkaDStream.flatMap(_._2.split(" "))
    // 每5秒钟统计过去30秒的
    val wordCounts = words.map((_, 1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(30), Seconds(5))
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
