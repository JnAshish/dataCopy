import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import scala.collection.JavaConverters._

object kafkaConsumer {

  def main(args: Array[String]): Unit = {

    val recordNum="100"

    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("group.id", "consumer-group")
    props.put("max.poll.records",recordNum)

    val consumer = new KafkaConsumer[String, String](props)

    val consumerTopic = "payment"
    val producerTopic = "copytest"

      consumer.subscribe(util.Arrays.asList(consumerTopic))

    try {
      while (true) {
  //      println("Hi")
        val records = consumer.poll(100)
    //    println("records: " + records)
        for (record <- records.asScala.iterator) {
          println("Value:" + record.value())
          kafkaProducer.send(producerTopic, record.key(), record.value())
          consumer.close()

        }
      }
  }
    catch {
      case ise: IllegalStateException =>
    {println(ise)
    sys.exit()}
    }
  }
}
