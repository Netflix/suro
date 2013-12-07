package com.netflix.suro.sink.kafka

import java.util.Properties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.metrics.KafkaMetricsReporter
import kafka.utils.VerifiableProperties

class KafkaProducer(props: Properties) {
  private val producer = new Producer[java.lang.Long, Array[Byte]](new ProducerConfig(props))
  private val messageBuffer = new collection.mutable.ArrayBuffer[KeyedMessage[java.lang.Long, Array[Byte]]]()

  KafkaMetricsReporter.startReporters(new VerifiableProperties(props))

  def send(keyedMsgList: java.util.List[KeyedMessage[java.lang.Long, Array[Byte]]]) {
    messageBuffer.clear()

    val i = keyedMsgList.iterator()
    while (i.hasNext) {
      messageBuffer += i.next()
    }

    producer.send(messageBuffer: _*)
  }

  def send(msg: KeyedMessage[java.lang.Long, Array[Byte]]) {
    producer.send(msg)
  }

  def close {
    producer.close()
  }
}