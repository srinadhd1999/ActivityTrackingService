package services

import models.ActivityRepository
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Properties, UUID}
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import play.api.libs.json._


@Singleton
class KafkaMessageConsumer @Inject()(config: Configuration, activityRepository: ActivityRepository, lifecycle: ApplicationLifecycle)(implicit ec: ExecutionContext) {

  private val kafkaConsumerProps: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get[String]("kafka.bootstrap.servers"))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.get[String]("kafka.group.id"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  private def generateTimestamp: String = {
    val currentDateTime: LocalDateTime = LocalDateTime.now()
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formattedDateTime: String = currentDateTime.format(formatter)
    formattedDateTime
  }

  private def idGenerator: String = {
    val uuid = UUID.randomUUID()
    val shortId = uuid.toString.take(8)  // Take the first 8 characters
    shortId
  }

  private def parseJson(jsonString: String): String = {
    val json: JsValue = Json.parse(jsonString)
    val resultMap: Map[String, String] = json.as[Map[String, String]]
    println(resultMap)
    resultMap.get("operation") match {
      case Some("add_new") =>
        s"added new product with product_id ${resultMap.getOrElse("product_id", "unknown")} and quantity of ${resultMap.getOrElse("add_quantity", "unknown")} at location ${resultMap.getOrElse("location", "unknown")}"
      case Some("delete_current") =>
        s"deleted product with id ${resultMap.getOrElse("product_id", "unknown")} from location ${resultMap.getOrElse("location", "unknown")}"
      case Some("increase_current") =>
        s"added quantity ${resultMap.getOrElse("increase_content", "unknown")} to product with id ${resultMap.getOrElse("product_id", "unknown")} at location ${resultMap.getOrElse("location", "unknown")}"
      case Some("decrease_current") =>
        s"removed quantity ${resultMap.getOrElse("decrease_content", "unknown")} from product with id ${resultMap.getOrElse("product_id", "unknown")} at location ${resultMap.getOrElse("location", "unknown")}"
      case _ =>
        "Unknown operation"
    }
  }

  private val consumer = new KafkaConsumer[String, String](kafkaConsumerProps)
  consumer.subscribe(List(config.get[String]("kafka.topic")).asJava)

  def receiveMessages(): Future[Unit] = Future {
    println("Started receiving messages")
    try {
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(100))
        for (record <- records.asScala) {
          try {
            println(s"Message content: ${record.value()}")
            activityRepository.add(idGenerator, parseJson(record.value()), generateTimestamp)
          } catch {
            case e: Exception =>
              println(s"Error processing record: ${record.value()}, error: ${e.getMessage}")
          }
        }
      }
    } catch {
      case e: Exception =>
        println(s"Error while consuming messages: ${e.getMessage}")
    } finally {
      consumer.close() // Close the Kafka consumer when done
    }
  }

  lifecycle.addStopHook { () =>
    Future.successful(consumer.close())
  }

  receiveMessages()
}
