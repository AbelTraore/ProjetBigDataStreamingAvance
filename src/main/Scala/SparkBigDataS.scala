import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf

import java.util.logging.LogManager
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext


object SparkBigDataS {

  var ss: SparkSession = null
  var spConf: SparkConf = null

 // private var trace_log : Logger = LogManager.getLogger("Log_Console")


  def main(args: Array[String]) : Unit = {
    val sc = Session_Spark(true)
    println("Ma session spark streaming avancée est crée")
  }




  /**
   * fonction qui initialise et instancie une session spark
   *
   * @param Env : c'est une variable qui indique l'environnement sur lequel notre application est déployée
   *            si Env = true, alors l'application est déployée en local, sinon, elle est déployée sur un cluster
   */

  def Session_Spark(Env: Boolean = true): SparkSession = {
    if (Env == true) {
      System.setProperty("hadoop.home.dir", "C:/Hadoop")
      ss = SparkSession.builder()
        .master(master = "local[*]")
        .config("spark.sql.crossJoin.enabled", "true")
        //.enableHiveSupport()
        .getOrCreate()
    } else {
      ss = SparkSession.builder()
        .appName(name = "Mon application Spark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
       // .enableHiveSupport()
        .getOrCreate()
    }
    return ss
  }

  /**
   * fonction qui initialise le contexte Spark Streaming
   * @param Env: Environnement sur lequel est déployé notre application. Si true, alors on est en localhost
   * @param duree_batch : c'est le SparkStreamingBatchDuration - où la durée du micro-batch
   * @return : la fonction renvoie en résultat une instance du contexte Streaming
   */

  def getSparkStreamingContext(Env: Boolean = true, duree_batch: Int): StreamingContext = {

    if (Env) {
      spConf = new SparkConf().setMaster("local[*]")
        .setAppName("Mon application streaming")
    }else {
      spConf = new SparkConf().setAppName("Mon application streaming")
    }
   // trace_log.info(s"la durée du micro-batch Spark est définie à : ${duree_batch secondes}")
    val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

    return ssc

  }
}

