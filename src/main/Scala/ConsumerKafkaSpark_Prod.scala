import KafkaStreaming.{ConsommateurKafka, KafkaParam, getKafkaSparkConsumerParams}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}

import java.time.Duration
import java.util.Properties
import java.util.Collections
import SparkBigDataS._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession


object ConsumerKafkaSpark_Prod {

  //private var trace_kafka : Logger = LogManager.getLogger("Log_Console")

  def main(args: Array[String]): Unit = {

    val ssc : StreamingContext = getSparkStreamingContext(true, 2)
    val kk_Consumer = getConsommateurKafka(ssc, Array("orderline"))

    kk_Consumer.foreachRDD {

      rdd_kafka => {
        val offsets_kafka = rdd_kafka.asInstanceOf[HasOffsetRanges].offsetRanges
        val data_kafka = rdd_kafka.map(e => e.value())

        //val ss = SparkSession.builder().config(rdd_kafka.sparkContext.getConf).getOrCreate()

        data_kafka.foreach {
          e => println(e)
        }

        kk_Consumer.asInstanceOf[CanCommitOffsets].commitAsync(offsets_kafka)

      }

    }

    ssc.start()
    ssc.awaitTermination()



  }

  def getKafkaSparkConsumerParams () : Map[String, Object] = {
  val KafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "abt5",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    return  KafkaParam
  }

  def getConsommateurKafka(StreamContext : StreamingContext, KafkaTopics : Array[String] ) : InputDStream[ConsumerRecord[String, String]] = {



     val KafkaParametres = getKafkaSparkConsumerParams()
    val ConsommateurKafka = KafkaUtils.createDirectStream[String, String](
        StreamContext,
        PreferConsistent,
        Subscribe[String, String](KafkaTopics, KafkaParametres)
      )

return ConsommateurKafka
  }


}
