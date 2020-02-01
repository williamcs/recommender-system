package com.client.client

import com.client.RecordProcessorTrait
import org.apache.kafka.clients.consumer.ConsumerRecord

class RecordProcessor extends RecordProcessorTrait[Array[Byte], Array[Byte]] {
  override def processRecord(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    val string = new String(record.value())
    println(s"Get Message: ${string}")
  }
}
