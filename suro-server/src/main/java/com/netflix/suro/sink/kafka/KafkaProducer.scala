package com.netflix.suro.sink.kafka

import java.util.Properties
import com.netflix.suro.message.Message
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import com.netflix.suro.message.serde.SerDe
import kafka.metrics.KafkaMetricsReporter
import kafka.utils.VerifiableProperties

class KafkaProducer(props: Properties) {
  private val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))
  private val messageBuffer = new collection.mutable.ArrayBuffer[KeyedMessage[String, Array[Byte]]]()

  KafkaMetricsReporter.startReporters(new VerifiableProperties(props))

  def send(msgList: java.util.List[Message], serde: SerDe[Message]) {
    messageBuffer.clear()

    val i = msgList.iterator()
    while (i.hasNext) {
      val m = i.next()
      messageBuffer += new KeyedMessage[String, Array[Byte]](m.getRoutingKey, serde.serialize(m))
    }

    producer.send(messageBuffer: _*)
  }

  def send(keyedMsgList: java.util.List[KeyedMessage[String, Array[Byte]]]) {
    messageBuffer.clear()

    val i = keyedMsgList.iterator()
    while (i.hasNext) {
      messageBuffer += i.next()
    }

    producer.send(messageBuffer: _*)
  }

  def close {
    producer.close()
  }

}
