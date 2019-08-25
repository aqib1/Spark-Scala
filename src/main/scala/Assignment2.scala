import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Assignment2 {
  def main(args: Array[String]) {
    val pathOfFile = "C:/Users/Aqib_Javed/Desktop/data/train.csv";
    val countColumnName = "is_booking"

    val sparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("MostPopularCountryObject")
      .getOrCreate();
    val ds = sparkSession.read
      .format("csv")
      .option("header", true)
      .load(pathOfFile);

    ds.select(col("*"))
      .filter("is_booking == 1 and srch_destination_id == hotel_country")
      .groupBy("hotel_country")
      .agg(count(countColumnName) as "popularity")
      .orderBy(desc("popularity"))
      .limit(1).show();


  }
}
