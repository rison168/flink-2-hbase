package com.rison.flink.map

import com.rison.flink.client.HbaseClient
import com.rison.flink.domain.LogEntity
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration

/**
 * @author : Rison 2021/6/13 下午4:59
 *
 */
case class UserInterestFunction() extends RichMapFunction[LogEntity, LogEntity]{
  var state: ValueState[Action] = _

  override def open(parameters: Configuration): Unit = {
    // 设置 state 的过期时间为100s
    val ttlConfig: StateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(100L))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val desc = new ValueStateDescriptor[Action]("action time", classOf[Action])
    desc.enableTimeToLive(ttlConfig)
    state = getRuntimeContext.getState(desc)

  }

  def saveToHBase(in: LogEntity, times: Int) = {
    if (in != null){
      for (i <- 0 until times){
        HbaseClient.incrementColumn("user_interest", in.userId.toString, "product", in.productId.toString)
      }
    }
  }

  def getTimesByRule(actionLastTime: Action, actionThisTime: Action): Int = {
    // 动作主要有3种类型
    // 1 -> 浏览  2 -> 分享  3 -> 购物
    val a1: Int = actionLastTime.`type`.toInt
    val a2: Int = actionThisTime.`type`.toInt
    val t1: Int = actionLastTime.time.toInt
    val t2: Int = actionThisTime.time.toInt
    var pluse = 1
    // 如果动作连续发生且时间很短(小于100秒内完成动作), 则标注为用户对此产品兴趣度很高
    if (a2 > a1 && (t2 - t1) < 100000L) pluse *= a2 - a1
    pluse
  }

  override def map(in: LogEntity): LogEntity = {
    var actionLastTime: Action = state.value()
    val actionThisTime = Action(in.action, in.time.toString)
    var times = 1
    if (actionLastTime == null){
      state.update(actionThisTime)
      actionLastTime = state.value()
      saveToHBase(in, 1)
    }else{
      times = getTimesByRule(actionLastTime, actionThisTime)
      saveToHBase(in, times)
    }
    if (actionThisTime.`type`.equals("3")){
      state.clear()
    }
    in
  }

}

case class Action(`type`: String, time: String)