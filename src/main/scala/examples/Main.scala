package examples

object Main {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark SQL examples")
      .getOrCreate;

    val schools = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/Schools.csv")

    //System.out.println(schools.show())
    val players = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/SchoolPlayers.csv")

    System.out.println(players.show())
  }

}
