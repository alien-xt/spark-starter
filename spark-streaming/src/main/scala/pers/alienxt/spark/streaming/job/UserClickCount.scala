package pers.alienxt.spark.streaming.job

import kafka.serializer.StringDecoder
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import pers.alienxt.spark.common.utils.JsonUtils
import pers.alienxt.spark.streaming.pojo.UserEvent
import redis.clients.jedis.JedisPool

/**
 * <p>文件描述：UserClickCount</p>
 * <p>内容摘要： 统计用户的点击次数
 *                          1.从Kafka里读取用户点击日志
 *                          2.进行统计，并将统计结果放在Redis里</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午10:12
 */
object UserClickCount {

  val appName = "UserClickCount"

  def main(args: Array[String]): Unit = {
    // local model
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    if (masterUrl.contains("local")) {
      // 屏蔽日志
      // Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      // Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    }

    // conf
    val conf = new SparkConf
    conf.setMaster("local[1]")
    conf.setAppName(appName)
    conf.set("SPARK_EXECUTOR_MEMORY", "2g")

    // init streaming context 每5秒作为一个批次
    val ssc = new StreamingContext(conf, Seconds(5))

    // 设置kafka信息
    val topics = Set("t_user_events")
    val brokers = "localhost:9092"
    val groupId = "g_t_user_events"
    val kafkaParams = Map[String, String] (
      "group.id" -> groupId,
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )

    // 创建DStream
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    // Redis index and key
    val dbIndex = 1
    val clickHashKey = "app::users::click"

    val events = kafkaDStream.flatMap(line => {
      val data = JsonUtils.fromJson(line._2, classOf[UserEvent])
      Some(data)
    })

    // 统计并更新Redis内的数据
    val userClicks = events.map(x => (x.getUid, x.getClickCount)).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {

          /**
           * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
           */
          object InternalRedisClient extends Serializable {

            @transient private var pool: JedisPool = null

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                         testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if (pool == null) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

                val hook = new Thread {
                  override def run = pool.destroy()
                }
                sys.addShutdownHook(hook.run)
              }
            }

            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }

          // Redis 配置
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "localhost"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          // 创建连接池
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val uid = pair._1
          val clickCount = pair._2
          val jedis = InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
