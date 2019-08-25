import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Assignment1 {

  def main(args: Array[String]) {
    val pathOfFile = "C:/Users/Aqib_Javed/Desktop/data/train.csv";
    val countColumnName = "is_booking";

    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Assignment1")
      .getOrCreate();

    val s = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .load(pathOfFile);

    s.select(s.col("*"))
      .filter("is_booking == 1")
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .agg(count(countColumnName) as "popularity")
      .orderBy(desc("popularity"))
      .limit(3)
      .show();
  }
}
