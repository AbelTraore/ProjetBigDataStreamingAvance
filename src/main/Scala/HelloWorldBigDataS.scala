import org.apache.avro.ipc.specific.Person
import org.apache.log4j._

import scala.tools.nsc.doc.model.Public
import scala.collection.mutable
import org.apache.avro.ipc.specific.Person

import scala.tools.nsc.doc.model.Public
import scala.collection.mutable._
import org.apache.log4j._

import java.io.FileNotFoundException
import scala.io._

//import PackageTest.PBGS_avancee.ClassTest



object HelloWorldBigDataS {

    /* premier programme */
    val ma_var_imm: String = "Abel" //variable immutable


    class Person (var nom : String, var prenom : String, var age : Int )

    private val une_var_imm: String = "Formation Big Data" /* variable à portée privée */
    BasicConfigurator.configure()
    private val trace_appli : Logger = LogManager.getLogger("Logger_Console")


    def main(args: Array[String]): Unit = {


     // val tt : ClassTest = new ClassTest
     // val ttt : Int = tt.comptage_package("essai")

      val diviseur : Double = try {
        division(12, 0)
      } catch {
        case ex : ArithmeticException => 0
        case ex2 : IllegalArgumentException => 0
      }
      trace_appli.info(s"la valeur de votre division est de ${diviseur}")

      lecture_fichier("c:/programmes/mesdonnees.txt")
      val nombre : Int = convert_entier("10")
      trace_appli.info(s"la valeur de votre nombre converti est : ${nombre}")

      println("Hello World : mon premier programme scala")

      var test_mu: Int = 15 //variable mutable
      test_mu = test_mu + 10

      println(test_mu)

      val test_imm: Int = 15
      //test_imm = 10 + 2

      //println(test_imm)
      println("Votre texte contient :" + comptage_caracteres(texte = "qu'avez vous mangé ce matin ?") + " caractères")
      getResultat(parametre = 10)
      testWhile(valeur_cond = 10)
      testFor()
      collectionScala ()
      collectionTuple()

    }

    //ma première fonction
    def comptage_caracteres(texte: String): Int = {

      trace_appli.info("demarrage du traçage de la classe")
      trace_appli.info(s"le paramètre tracé par Log4J pour cette fonction est: $texte")
      trace_appli.warn(s"Message d'avertissement Log4J interpolation de chaines: ${10 + 15}")

      if (texte.isEmpty){
        0
      }else {
        texte.trim.length()
      }
    }

    //syntaxe 2
    def comptage_caracteres2(texte: String): Int = {
      return texte.trim.length()

    }

    //syntaxe 3
    def comptage_caracteres3 (texte : String) : Int = texte.trim.length()

    //ma première méthode/procédure
    def getResultat (parametre : Any) : Unit = {
      if (parametre == 10) {
        println("Votre valeur est un entier")
      } else {
        println("Votre valeur n'est pas un entier")
      }
    }

    //structures conditionnelles
    def testWhile (valeur_cond : Int) : Unit = {
      var i : Int = 0
      while (i < valeur_cond) {
        println("itération while N°" +i)
        i = i+1
      }
    }

    def testFor () : Unit = {
      var i : Int = 0
      for (i <- 5 to 15) {
        println("itération For N°" +i)
      }
    }
    // Ma première fonction
    def comptage_caracteres4(texte: String): Int = {
      texte.trim.length()
    }

    //les collections en scala
    def collectionScala () : Unit = {

      val maliste: List[Int] = List(1, 2, 3, 5, 10, 45, 15)
      val liste_s: List[String] = List("julien", "Paul", "jean", "rac", "trec", "joel", "ed", "chris", "maurice")
      //val chaine : String = "ma_chaine"
      //chaine.toList()
      val plage_v: List[Int] = List.range(1, 15, 2)
      println(maliste(0))
      //println(liste_s(4))

      for (i <- liste_s) {
        println(i)
      }

      //manipulation des collections à l'aide des fonctions anonymes
      val resultats: List[String] = liste_s.filter(e => e.endsWith("n"))

      for (r <- resultats) {
        println(r)
      }

      val res: Int = liste_s.count(i => i.endsWith("n"))
      println("Nombre d'éléments respectant la condition : " + res)

      val maliste2: List[Int] = maliste.map(e => e * 2)
      for (r <- maliste2) {
        println(r)
      }

      val maliste3: List[Int] = maliste.map((e: Int) => e * 2)
      val maliste4: List[Int] = maliste.map(_ * 2)

      val nouvelle_liste: List[Int] = plage_v.filter(p => p > 5)

      val new_liste: List[String] = liste_s.map(s => s.capitalize)

      new_liste.foreach(e => println("New liste : " + e))

      nouvelle_liste.foreach(e => println("Nouvelle liste : " + e))
      plage_v.foreach(println(_))
    }
    def collectionTuple() : Unit = {

      val tuple_test = (45, "abt", "False")
      println(tuple_test._3)

      val nouv_personne : Person = new Person ( nom = "TRAORE", prenom = "Abel", age = 41 )
      val tuple_2 = ("test", nouv_personne, 67 )

      tuple_2.toString().toList
    }

    //table de hachage
    val states = Map(
      "AK" -> "Alaska",
      "IL" -> "Illinois",
      "KY" -> "Kentucky"
    )

    val personne = Map(
      "nom" -> "TRAORE",
      "prenom" -> "Abel",
      "age" -> 41
    )

    //les tableaux ou Array
    val montableau : Array[String] = Array("juv","jvc", "test")
    montableau.foreach(e => println(e))

    def convert_entier (nombre_chaine : String) : Int = {
      try {
        val nombre: Int = nombre_chaine.toInt
        return nombre
      } catch {
        case ex : Exception => 0

      }

    }

    def lecture_fichier(chemin_fichier : String) : Unit = {
      try {
        val fichier = Source.fromFile(chemin_fichier)
        fichier.getLines()
        fichier.close()
      } catch {
        case ex : FileNotFoundException => trace_appli.error("votre fichier est introuvable. Vérifiez le chemin d'accès" + ex.printStackTrace())
      }

    }

    def division(numerateur : Int, denominateur : Int) : Double = {
      val resultat = numerateur/denominateur
      return resultat
    }








  }







