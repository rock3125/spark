

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


// need to add this to JVM options: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
fun main(args: Array<String>) {

    val sparkConf = SparkConf().setAppName("SparkFirstApp").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val dataFrame1: Dataset<Row> = sparkSession
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
        .load("data/100-sales-records.csv")

    dataFrame1.createTempView("salesRecord1");
    val q1 = sparkSession.sql("select * from salesRecord1");
    q1.show();

}
