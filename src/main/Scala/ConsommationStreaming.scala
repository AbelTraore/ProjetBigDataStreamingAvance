import KafkaStreaming._
import SparkBigDataS._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object ConsommationStreaming {

  val bootStrapServers : String = ""
  val consumerGroupId : String = ""
  val consumerReadOrder : String = ""
  val zookeeper : String = ""
  val  kerberosName : String = ""
  val batchDuration : Int = 15
  val topics : Array[String] = Array("")
  val checkpointChemin : String = "/Hadoop/mhgb/datalake/"

  val schema_Kafka = StructType ( Array(
    StructField("Zipcode", IntegerType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true)

  ))


  private var trace_consommation : Logger = LogManager.getLogger("Log_Console")

  /**
   *checkpointing avec Spark Streaming
   * @param checkpointPath : chemin d'enregistrement du checkpoint
   * @return : context spark streaming avec prise en compte du checkpoint
   */
  def fault_tolerant_SparkStreamingContext (checkpointPath : String) : StreamingContext = {

    val ssc2 = getSparkStreamingContext(true, batchDuration)
    val kafkaStreams_cp = getConsommateurKafka(bootStrapServers, consumerGroupId, consumerReadOrder,
      zookeeper, kerberosName, topics, ssc2)

    ssc2.checkpoint(checkpointPath)

    return ssc2

  }


  def main(args: Array[String]): Unit = {

    val ssc = getSparkStreamingContext(true, batchDuration)

    val ssc_cp = StreamingContext.getOrCreate(checkpointChemin, () => fault_tolerant_SparkStreamingContext(checkpointChemin))

    val kafkaStreams = getConsommateurKafka(bootStrapServers, consumerGroupId, consumerReadOrder,
      zookeeper, kerberosName, topics, ssc)

    val kafkaStreams_cp = getConsommateurKafka(bootStrapServers, consumerGroupId, consumerReadOrder,
      zookeeper, kerberosName, topics, ssc_cp)

   //première méthode : val dataStreams = kafkaStreams.map(record => record.value())

    //deuxième méthode (recommandée)

    kafkaStreams_cp.checkpoint(Seconds(15))

    kafkaStreams.foreachRDD {
      rddKafka => {
        if(!rddKafka.isEmpty()) {

          val offsets = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges
          val dataStreams = rddKafka.map(record => record.value())

          val ss = SparkSession.builder().config(rddKafka.sparkContext.getConf).enableHiveSupport().getOrCreate()
          import ss.implicits._

          val df_kafka = dataStreams.toDF("tweet_message")

          df_kafka.createOrReplaceTempView("kafka_events")

          //1 ère méthode d'exploitation du Data Frame et SQL avec Kafka
          val df_eventsKafka = ss.sql("select * from kafka_events") //lancer une requête sql

          df_eventsKafka.show()

          //2 ème méthode d'exploitation du Data Frame et SQL avec Kafka
          val df_eventsKafka_2 = df_kafka.withColumn("tweet_message", from_json(col("tweet_message"), schema_Kafka))
            .select(col("tweet_message.*"))  //quand on ne connait pas le schema des données

          // sémantique de livraison et de traitement exactement une fois. Persistance des offsets dans Kafka
          trace_consommation.info("persistance des offsets dans Kafka encours....")
          kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
          trace_consommation.info("persistance des offsets dans Kafka terminé avec succès ! :)")


        }
      }
    }

    // gestion des offsets
    kafkaStreams.foreachRDD{
      rddKafka => {
        if (!rddKafka.isEmpty()) {

          val offsets = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges

          val datastreams = rddKafka.map(event => event.value())

          // lire les offsets
          for(o <- offsets) {
            println(s"Le topic lu est : ${o.topic}, la partition est : ${o.fromOffset}, l'offset de fin est : ${o.untilOffset}")
            //ou encore
            //trace_consommation.info()
          }

        }

      }
    }


    ssc.start()
    ssc.awaitTermination()

    //ssc_cp.start()
    //ssc_cp.awaitTermination()



  }

  def streamingCas () : Unit = {

    val ssc = getSparkStreamingContext(true, batchDuration)



    ssc.start()
    ssc.awaitTermination()
  }


}
