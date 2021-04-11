package com.skonuniverse.flink.datatype

import java.sql.Timestamp

sealed trait RareMessage {
  def eventTime: Timestamp
  def fakeness: Boolean
}

case class DefaultRareMessage(eventTime: Timestamp,
                              fakeness: Boolean = false,
                              key: String = "",
                              value: Double = Double.NegativeInfinity)
    extends RareMessage
