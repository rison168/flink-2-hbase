package com.rison.flink.top

import com.rison.flink.domain.TopProductEntity
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author : Rison 2021/6/15 上午10:25
 *
 */
case class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, TopProductEntity, List[TopProductEntity]]{
  lazy val itemState = getRuntimeContext.getListState(new ListStateDescriptor[TopProductEntity]("item-state", classOf[TopProductEntity]))
  override def processElement(i: TopProductEntity, context: KeyedProcessFunction[Long, TopProductEntity, List[TopProductEntity]]#Context, collector: Collector[List[TopProductEntity]]): Unit = {
    itemState.add(i)
    //注册windowEnd + 1 的 event Timer ,当触发时，说明收齐了windowEnd窗口所有的商品数据
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TopProductEntity, List[TopProductEntity]]#OnTimerContext, out: Collector[List[TopProductEntity]]): Unit = {
    val list = new ListBuffer[TopProductEntity]()
    itemState.get().forEach(
      data => {
        list.append(data)
      }
    )
    //提前清除状态中的数据，释放空间
    itemState.clear()
    //按照点击量从大到小排序
    val topProductList: List[TopProductEntity] = list.sortBy(_.actionTimes).toList.take(topSize)
    out.collect(topProductList)
  }
}
