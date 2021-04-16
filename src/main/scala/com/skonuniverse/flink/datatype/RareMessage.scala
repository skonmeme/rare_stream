package com.skonuniverse.flink.datatype

import java.sql.Timestamp

case class RareMessage(eventTime: Timestamp,
                       key: Int = -1,
                       value: Double = Double.NegativeInfinity)
