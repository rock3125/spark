

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


@SpringBootApplication
open class Main


// need to add this to JVM options: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
fun main(args: Array<String>) {

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
            {"type": "record", "name": "content_collection", "fields": [
                {"name": "word", "type": "string"}, 
                {"name": "metadata", "type": "string"}, 
                {"name": "created", "type": "long"}, 
                {"name": "url_id", "type": "int"},
                {"name": "source_id", "type": "int"},
                {"name": "acl_hash", "type": "int"},
                {"name": "sentence", "type": "int"},
                {"name": "offset", "type": "int"},
                {"name": "tag", "type": "string"},
                {"name": "score", "type": "float"},
                {"name": "is_entity", "type": "boolean"}
            ]}
        """.trimIndent()
    )
    val schema = Schema.Parser().parse(sb.toString())

    val pw = AvroParquetWriter
        .builder<Any>(ParquetWriter.outputFile("data/test.parquet"))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

    val indexRecord = GenericData.Record(schema)
    indexRecord.put("word", "test")
    indexRecord.put("metadata", "{body}}")
    indexRecord.put("created", System.currentTimeMillis())
    indexRecord.put("url_id", 1)
    indexRecord.put("source_id", 1)
    indexRecord.put("acl_hash", "12345")
    indexRecord.put("sentence", 1)
    indexRecord.put("offset", 1)
    indexRecord.put("tag", "NNP")
    indexRecord.put("score", 1.0f)
    indexRecord.put("is_entity", false)

    try {
        pw.write(indexRecord)
    } catch (ex: ClassCastException) {
        logger.info("documentRecord.write($indexRecord): ${ex.message ?: ex}")
    }

    pw.close()

}
