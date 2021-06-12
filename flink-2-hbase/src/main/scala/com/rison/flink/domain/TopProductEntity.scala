package com.rison.flink.domain

/**
 * @author : Rison 2021/6/12 下午12:36
 *
 */
case class TopProductEntity(productId: Int, actionTimes: Int, windowEnd: Long, rankName: String)
