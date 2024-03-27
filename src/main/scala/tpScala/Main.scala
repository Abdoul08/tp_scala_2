package tpScala
import tpScala.commons.sparkutils.get_sparkSession
//import org.apache.spark.sql.DataFrame

object Main {

 def main(args: Array[String]): Unit ={

  val spark = get_sparkSession("tp")

  // Charger le fichier texte
  val texteSophie = spark.read.textFile("/opt/spark-data/sophie.txt")

  // Nombre de lignes dans le Dataset
  val count = texteSophie.count()
  println("------------------------------------------")
  println(s"Nombre de lignes dans ce Dataset : $count")

  // Première ligne du Dataset
  val firstLine = texteSophie.first()
  println("------------------------------------------")
  println(s"Première ligne de ce Dataset : $firstLine")

  // 5 premières lignes du Dataset
  val firstFiveLines = texteSophie.take(5)
  println("------------------------------------------")
  println("5 premières lignes de ce Dataset :")
  firstFiveLines.foreach(println)

  // Retourner une partie du contenu du Dataset
  val allLines = texteSophie.collect()
  println("------------------------------------------")
  println("Contenu du Dataset :")
  allLines.foreach(println)

  // Lignes contenant "poupee"
  val lignesAvecPoupee = texteSophie.filter(line => line.contains("poupee"))

  // Les 2 premières lignes contenant "poupee"
  val twoLinesWithPoupee = lignesAvecPoupee.take(2)
  println("------------------------------------------")
  println("Les 2 premières lignes contenant 'poupee' :")
  twoLinesWithPoupee.foreach(println)

  // Nombre de lignes contenant "poupee"
  val countPoupee = texteSophie.filter(line => line.contains("poupee")).count()
  println("------------------------------------------")
  println(s"Nombre de lignes contenant 'poupee' : $countPoupee")

  // Arrêt de la session Spark
  spark.stop()

 }
}
