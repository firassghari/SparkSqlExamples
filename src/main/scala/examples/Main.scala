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
    val AvgYearMinPerSchoolId = getAvgYearMinPerSchoolId(players, spark)
  //  System.out.println(AvgYearMinPerSchoolId.show())
    joinSchoolAndSchoolPlayers(schools,players,spark).show()

  }
  def joinSchoolAndSchoolPlayers(schools:DataFrame,schoolPlayers:DataFrame , session: SparkSession):DataFrame =
  {
    import org.apache.spark.sql.functions._
    schoolPlayers.join(schools , schoolPlayers.col("schoolID")===schools.col("schoolID"))
  }
  def getAvgYearMinPerSchoolId(schools:DataFrame , session:SparkSession): DataFrame =
  {
    import org.apache.spark.sql.functions._

    schools.groupBy("schoolID" ).agg(avg("yearMin").as("hello"))

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
