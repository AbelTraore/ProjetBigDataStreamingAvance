import org.apache.zookeeper.server.auth.KerberosName
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.protocol._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization._
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import SparkBigDataS._
import org.apache.spark.streaming.dstream.InputDStream
import org.glassfish.hk2.api.messaging.Topic

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.log4j.{LogManager, Logger}
import org.apache.log4j
//import org.apache.spark.streaming.Duration

import scala.tools.nsc.interactive.Logger
//import scala.tools.nsc.interpreter.replProps.info
import java.util.Collections
import scala.collection.JavaConverters._
import java.time.Duration


object KafkaStreaming {

var KafkaParam : Map[String, Object] = Map(null, null)
var ConsommateurKafka : InputDStream[ConsumerRecord[String, String]] = null
//private var trace_kafka : Logger = LogManager.getLogger("Log_Console")

  def getKafkaSparkConsumerParams (kafkaBootStrapServers : String,
                      KafkaConsumerGroupId : String,
                      KafkaConsumerReadOrder : String,
                      KafkaZookeeper : String,
                     KerberosName : String) : Map[String, Object] = {
    KafkaParam = Map(
      "boostrap.servers" -> kafkaBootStrapServers,
      "group.id" -> KafkaConsumerGroupId,
      "zookeeper.hosts" -> KafkaZookeeper,
      "auto.offset.reset" -> KafkaConsumerReadOrder,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" -> KerberosName,
      "security.protocol" -> SecurityProtocol.PLAINTEXT
    )

    return  KafkaParam
  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param KafkaConsumerGroupId
   * @param KafkaConsumerReadOrder
   * @param KafkaZookeeper
   * @param KerberosName : service kerberos
   * @param batchDuration : la durée du streaming
   * @param KafkaTopics : le nom des topics
   * @return
   */

  def getConsommateurKafka(kafkaBootStrapServers : String,
                           KafkaConsumerGroupId : String,
                           KafkaConsumerReadOrder : String,
                           KafkaZookeeper : String,
                           KerberosName : String,
                           batchDuration : Int,
                           KafkaTopics : Array[String]) : InputDStream[ConsumerRecord[String, String]] = {

    val ssc = getSparkStreamingContext(true, batchDuration)
    KafkaParam = getKafkaSparkConsumerParams(kafkaBootStrapServers, KafkaConsumerGroupId, KafkaConsumerReadOrder, KafkaZookeeper, KerberosName)


    val ConsommateurKafka : InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](KafkaTopics, KafkaParam)
    )

    return ConsommateurKafka

  }

def getKafkaProducerParams(KafkaBootStrapServers : String) : Properties = {
  val props : Properties = new Properties()
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("bootstrap.servers", KafkaBootStrapServers)
  props.put("security.protocol", "SASL_PLAINTEXT")

  return props
}

  def getKafkaConsumerParams(kafkaBootStrapServers : String,
                             KafkaConsumerGroupId : String) : Properties = {
    val props : Properties = new Properties()
    props.put("boostrap.servers", kafkaBootStrapServers)
    props.put("auto.offset.reset", "lastest")
    props.put("group.id", KafkaConsumerGroupId)
    props.put("enable.auto.commit", false: java.lang.Boolean)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    return props

  }


  def getClientConsumerKafka(kafkaBootStrapServers : String,
                             KafkaConsumerGroupId : String,
                             topic_list : String) : KafkaConsumer[String, String] = {

    //trace_kafka.info ("instanciation d'un consommateur kafka ...")
    val consumer = new KafkaConsumer[String, String](getKafkaConsumerParams(kafkaBootStrapServers, KafkaConsumerGroupId))

    try{

      consumer.subscribe(Collections.singletonList(topic_list))

      while(true) {
        val messages : ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(30))
        if (!messages.isEmpty) {
          //trace_kafka.info("Nombre de messages collectés dans la fenêtre:" +messages.count())
          for(message <- messages.asScala) {
            println("Topic: " + message.topic() +
              ",Key: " + message.key() +
              ",Value: " + message.value() +
              ", Offset: " + message.offset() +
              ", Partition: " + message.partition())
          }

          try {
            consumer.commitAsync()
          } catch {
            case ex : CommitFailedException =>
            //trace_kafka.error(s"erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons bien reçu les données")
          }

          //méthode de lecture 2
          /*
          val messageIterateur = messages.iterator()
          while (messageIterateur.hasNext == true) {
            val msg = messageIterateur.next()
            println(msg.key() + msg.value() + msg.offset() + msg.partition())
          } */
        }


      }

    } catch {
      case excpt : Exception =>
        //trace_kafka.error("erreur dans le consumer" + excpt.printStackTrace())
    } finally {
      consumer.close()
    }



    return consumer

  }

  /**
   * création d'un Kafka Producer qui va être déployé en production
   * @param KafkaBootStrapServers : agents Kafka sur lesquels publier le message
   * @param topic_name : topic dans lequel publier le message
   * @param message : message à publier dans le topic @topic_name
   * @return : renvoie Producer Kafka
   */



  def getProducerKafka (KafkaBootStrapServers : String, topic_name : String, message : String) : KafkaProducer[String, String] = {
    //trace_kafka.info("instanciation d'une instance du producer kafka aux serveurs" + ${KafkaBootStrapServers})
    lazy val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams(KafkaBootStrapServers))

   //trace_kafka.info(s"message à publier dans le topic ${topic_name}")
    val record_publish = new ProducerRecord[String, String](topic_name, message)

    try {
      //trace_kafka.info("publication du message")
      producer_Kafka.send(record_publish)
} catch {
  case ex : Exception =>
    //trace_kafka.error(s"erreur dans la publication du message dans kafka ${ex.printStackTrace()}")
    //trace_kafka.info(s"La liste des paramètres pour la connexion du producer kafka sont : ${getKafkaProducerParams(KafkaBootStrapServers)}")
} finally {
      println("n'oublier pas de clôturer le Producer à la fin de son utilisation")
  //producer_Kafka.close()
}

    return producer_Kafka
}


}
