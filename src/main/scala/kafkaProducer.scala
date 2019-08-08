import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object kafkaProducer {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  val producer = new KafkaProducer[String, String](props)

  def send(topic:String, key: String, records: String): Unit ={
    val data = new ProducerRecord[String, String] (topic, key, records)
    producer.send(data).get()
  }

}
