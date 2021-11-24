import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections
import scala.collection.JavaConverters._
import KafkaStreaming._
import org.apache.log4j.{LogManager, Logger}
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import SparkBigDataS._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.Minutes


//import scala.tools.nsc.interactive.Logger

class TwitterKafkaStreaming {

 private var trace_client_streaming : Logger = LogManager.getLogger("Log_Console")

  private def twitterOAuthConf (CONSUMER_KEY : String,
                                CONSUMER_SECRET : String,
                                ACCESS_TOKEN : String,
                                TOKEN_SECRET : String) : ConfigurationBuilder = {
    val twitterConfig = new ConfigurationBuilder()
    twitterConfig
      .setJSONStoreEnabled(true)
      .setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(TOKEN_SECRET)

    return twitterConfig
  }

  /**
   * Ce client est un client Hosebird. Il permet de collecter les tweets contenant une liste d'hashtag et
   * de le publier en temps réel dans un ou plusieurs topics Kafka
   * @param CONSUMER_KEY : la clé du consommateur pour l'authentification OAuth
   * @param CONSUMER_SECRET : le secret du consommateur pour l'authentification OAuth
   * @param ACCESS_TOKEN : le token d'accès pour l'authentification OAuth
   * @param TOKEN_SECRET : le token secret pour l'authentification OAuth
   * @param liste_hashtags : la liste des hashtags de tweets dont on souhaite collecter
   * @param KafkaBootStrapServers : la liste d'adresses IP (et leur port) des agents du cluster kafka
   * @param topic : le(s) topic(s) dans le(s)quel(s) stocker le tweets collectés
   */

def ProducerTwitterKafkaHBC (CONSUMER_KEY : String,
                             CONSUMER_SECRET : String,
                             ACCESS_TOKEN : String,
                             TOKEN_SECRET : String,
                             liste_hashtags : String,
                             KafkaBootStrapServers : String,
                             topic : String ) : Unit = {
  val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](1000)

  val auth : Authentication = new  OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET)

  val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
  endp.trackTerms(Collections.singletonList(liste_hashtags))
  endp.trackTerms(List(liste_hashtags).asJava)  //Collections.singletonList

  val constructeur_hbc : ClientBuilder = new  ClientBuilder()
    .hosts(Constants.STREAM_HOST)
    .authentication(auth)
    .gzipEnabled(true)
    .endpoint(endp)
    .processor(new StringDelimitedProcessor(queue)
    )

  val client_hbc : Client = constructeur_hbc.build()

  try {
    client_hbc.connect()

    while (!client_hbc.isDone) {
      val tweets : String = queue.poll(15, TimeUnit.SECONDS)
      getProducerKafka(KafkaBootStrapServers, topic, tweets) //intégration avec notre producer Kafka
      println("message Twitter : " + tweets)
  }
  } catch  {

    case ex : InterruptedException => trace_client_streaming.error("le client Twitter HBC a été interrompu à cause de cette erreur : " + ex.printStackTrace())

  } finally {

    client_hbc.stop()
    getProducerKafka(KafkaBootStrapServers, topic, "").close()

  }

}

  /**
   * Ce client Twitter4J récupère les données streaming de Twitter. Il est un peu différent du client HBC,
   * car il est plus vaste et plus complet
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET
   * @param requete
   * @param KafkaBootStrapServers
   * @param topic
   */
  def ProducerTwitter4JKafka (CONSUMER_KEY : String,
                               CONSUMER_SECRET : String,
                               ACCESS_TOKEN : String,
                               TOKEN_SECRET : String,
                              requete : String,
                               KafkaBootStrapServers : String,
                               topic : String ) : Unit = {

    val queue : BlockingQueue[Status] = new LinkedBlockingQueue[Status](10000)

    val twitterStream = new  TwitterStreamFactory(twitterOAuthConf(CONSUMER_KEY,
      CONSUMER_SECRET,
      ACCESS_TOKEN,
      TOKEN_SECRET).build()).getInstance()

    val listener = new  StatusListener {
      override def onStatus(status: Status): Unit = {

        trace_client_streaming.info("événement d'ajout de tweet détecté. Tweet complet : " + status.getText)
        queue.put(status)
        getProducerKafka(KafkaBootStrapServers, topic, status.getText) //première méthode
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
      override def onTrackLimitationNotice(i: Int): Unit = {}
      override def onScrubGeo(l: Long, l1: Long): Unit = {}
      override def onStallWarning(stallWarning: StallWarning): Unit = {}

      override def onException(e: Exception): Unit = {
        trace_client_streaming.error("Erreur générée par Twitter :" + e.printStackTrace())
      }

    }

    twitterStream.addListener(listener)
    //twitterStream.sample() //déclenche la réception des tweets

    val query = new  FilterQuery().track(requete)
    twitterStream.filter(query) //filtre les données

    //2ème méthode
   // while (true) {
   //   val tweet: Status = queue.poll(15, TimeUnit.SECONDS)
   //   getProducerKafka(KafkaBootStrapServers, topic, tweet.getText)
   // }
    getProducerKafka(KafkaBootStrapServers, "", "").close()
    twitterStream.shutdown()

    }

  /**
   * Client Spark Streaming Twitter Kafka. Ce client Spark Streaming se connecte à Twitter et
   * publie les infos dans Kafka via un Producer Kafka
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET
   * @param filtre
   * @param KafkaBootStrapServers
   * @param topic
   */
  def ProducerTwitterKafkaSpark (CONSUMER_KEY : String,
                                 CONSUMER_SECRET : String,
                                 ACCESS_TOKEN : String,
                                 TOKEN_SECRET : String,
                                 filtre : Array[String],
                                 KafkaBootStrapServers : String,
                                 topic : String ) : Unit = {

    val authO = new OAuthAuthorization(twitterOAuthConf(CONSUMER_KEY,
      CONSUMER_SECRET,
      ACCESS_TOKEN,
      TOKEN_SECRET).build())

    val client_Streaming_Twitter = TwitterUtils.createStream(getSparkStreamingContext(true,15),Some(authO), filtre)

    val tweetsmsg = client_Streaming_Twitter.flatMap(status => status.getText())
    val tweetsComplets = client_Streaming_Twitter.flatMap(status => (status.getText() ++ status.getContributors() ++ status.getLang()))
    val tweetsFR = client_Streaming_Twitter.filter(status => status.getLang() == "fr")
    val hastags = client_Streaming_Twitter.flatMap(status => status.getText().split("").filter(status => status.startsWith("#")))
    val hastagsFR = tweetsFR.flatMap(status => status.getText().split("").filter(status => status.startsWith("#")))
    val hastagsCount = hastagsFR.window(Minutes(3))

    //ATTENTION à cette erreur !!! getProducerKafka(KafkaBootStrapServers, topic, tweetsmsg.toString())

    //tweetsmsg.saveAsTextFiles("Tweets")
    //tweetsmsg.saveAsObjectFiles("Tweets", "json")

    //1ère méthode recommandée
    tweetsmsg.foreachRDD{
      (tweetsRDD, temps) => if (!tweetsRDD.isEmpty()) {
        tweetsRDD.foreachPartition{
          partitionsOfTweets =>
            val producer_Kafka = new KafkaProducer[String, String](getKafkaProducerParams(KafkaBootStrapServers))
            partitionsOfTweets.foreach {
              tweetEvent =>
                val record_publish = new ProducerRecord[String, String](topic, tweetEvent.toString)
                producer_Kafka.send(record_publish)
            }
            producer_Kafka.close()
        }
      }

      }

    //2ème méthode (je préfère cette méthode)
    try {
      tweetsComplets.foreachRDD{
        tweetsRDD =>
          if (!tweetsRDD.isEmpty()) {
            tweetsRDD.foreachPartition{
              tweetsPartition => tweetsPartition.foreach { tweets =>
                getProducerKafka(KafkaBootStrapServers, topic, tweets.toString)
              }
            }
          }
          getProducerKafka(KafkaBootStrapServers, topic_name = "", "").close()
      }
    }
    catch {
      case ex : Exception => trace_client_streaming.error(ex.printStackTrace())
    }
    //finally {}

    getSparkStreamingContext(true,15).start()
    getSparkStreamingContext(true,15).awaitTermination()

    getSparkStreamingContext(true,15).stop() //pour arrêter

  }


  }


  //exemple de spécification d'un paramètre optionnel en scala
  //test(Some(true), 15)

  //exemple de spécification d'un paramètre optionnel en scala
  //def test(var1: Some[Boolean], param2: Int) : Unit = {
  //println(var1)
  //}


