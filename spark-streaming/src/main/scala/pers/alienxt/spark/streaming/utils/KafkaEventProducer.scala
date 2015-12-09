package pers.alienxt.spark.streaming.utils

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import pers.alienxt.spark.common.utils.JsonUtils
import pers.alienxt.spark.streaming.pojo.UserEvent


/**
 * <p>文件描述：KafkaEventProducer</p>
 * <p>内容摘要：Kafka生产者，随即构建用户日志到kafka里</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午10:12
 */
object KafkaEventProducer {

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private var pointer = -1

  def getUserID() : String = {
    pointer = pointer + 1
    if (pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  def main(args: Array[String]) {
    val topic = "t_user_events"
    val brokers = "localhost:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)


    while(true) {
      val event = new UserEvent()
      event.setUid(getUserID)
      val json = JsonUtils.toJson(event)
      // produce event message
      producer.send(new KeyedMessage[String, String](topic, json))
      println("Message sent: " + json)
      Thread.sleep(2000)
    }
  }

}
