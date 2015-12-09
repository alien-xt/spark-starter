package pers.alienxt.spark.streaming.pojo

import scala.beans.BeanProperty
import scala.util.Random

/**
 * <p>文件描述：UserEvent</p>
 * <p>内容摘要： 用户点击日志对象</p>
 * <p>其他说明： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/12/8 上午10:12
 */
class UserEvent {

  private val random = new Random()

  @BeanProperty
  var uid: String = ""
  @BeanProperty
  val osType: String = "Android"
  @BeanProperty
  val clickCount: Int = click
  @BeanProperty
  val eventTime: String = System.currentTimeMillis.toString

  def click() : Int = {
    random.nextInt(10)
  }


}
