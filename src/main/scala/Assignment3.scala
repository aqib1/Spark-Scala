import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Assignment3 {

  def main(args: Array[String]) {
    val pathOfFile = "C:/Users/Aqib_Javed/Desktop/data/train.csv"
    val sparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Assignment3")
      .getOrCreate();

    val ds = sparkSession.read
      .format("csv")
      .option("header", true)
      .load(pathOfFile);

    ds.select(col("*"))
      .filter("srch_adults_cnt > 0")
      .filter("srch_children_cnt > 0")
      .filter("is_booking == 0")
      .filter("srch_rm_cnt > 0")
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .agg(count("srch_rm_cnt") as "searchPopularity")
      .orderBy(desc("searchPopularity"))
      .limit(3).show();
  }
}
