package examples

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark SQL examples")
      .getOrCreate;

    val schools = readCV("src/main/resources/Schools.csv",spark)
    val players = readCV("src/main/resources/SchoolsPlayers.csv",spark)

    
  }
  def readCV (path:String , session:SparkSession ):DataFrame  =
  {
     session.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(path)
  }

}
