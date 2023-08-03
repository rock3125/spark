

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J


@SpringBootApplication
open class Main


// need to add this to JVM options: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
fun main(args: Array<String>) {
    SysOutOverSLF4J.registerLoggingSystem("org.slf4j.simple.SimpleLogger")
    SysOutOverSLF4J.sendSystemOutAndErrToSLF4J()

    val logger = LoggerFactory.getLogger(Main::class.java)
    logger.info("test")

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

    val sb = StringBuilder()
    sb.append(
        """
            {"type": "record", "name": "sales_report", "fields": [
                {"name": "region", "type": "string"}, 
                {"name": "country", "type": "string"}, 
                {"name": "sold", "type": "int"}, 
                {"name": "price", "type": "float"}
            ]}
        """.trimIndent()
    )
    val schema = Schema.Parser().parse(sb.toString())

    val pw = AvroParquetWriter
        .builder<Any>(ParquetWriter.outputFile("data/test.parquet"))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

    // write all records to a parquet file for testing
    for (item in q1.toLocalIterator()) {

        val indexRecord = GenericData.Record(schema)
        indexRecord.put("region", item[0]?.toString() ?: "")
        indexRecord.put("country", item[1]?.toString() ?: "")
        indexRecord.put("sold", item[8]?.toString()?.toIntOrNull() ?: 0)
        indexRecord.put("price", item[9]?.toString()?.toFloatOrNull() ?: 0.0f)

        try {
            pw.write(indexRecord)
        } catch (ex: ClassCastException) {
            logger.info("documentRecord.write($indexRecord): ${ex.message ?: ex}")
        }
    }

    pw.close()

}
