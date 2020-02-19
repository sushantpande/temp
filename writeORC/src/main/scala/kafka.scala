package sf.ingest.kafka

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import requests._
import play.api.libs.json._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.format.SparkAvroConversions
import scala.util.parsing.json._
import sf.ingest.config._

object KafkaStream{
  def main(args: Array[String]){

    val spark = SparkSession
      .builder()
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.streaming.fileSink.log.cleanupDelay", 60000)
      .getOrCreate()

    val checkpointDirectory = args(0)
    val outputDirectory = args(1)
    val brokerServers = args(2)
    val brokerTopic = args(3)
    val schemaRegistry = args(4)

    val schemaKeys = Array[String]("_documentType","_plugin")
    var schemaMap:Map[String, StructType] = Map()
    var partitionMap:Map[String, Seq[String]] = Map()
    val schemaSource = "http://" + schemaRegistry + "/subjects/"

    println(checkpointDirectory, outputDirectory, brokerServers, brokerTopic)

    import spark.implicits._


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerServers)
      .option("subscribe", brokerTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .load()

      val yarnSchema = StructType(Array(
      StructField("node", StringType),
      StructField("_documentType", StringType),
      StructField("_plugin", StringType),
      StructField("level", StringType),
      StructField("time", TimestampType),
      StructField("_tag_appName", StringType),
      StructField("message", StringType),
      StructField("yarnApplication", StringType),
      StructField("yarnContainer", StringType)
      ))

    val defaultSchema = StructType(Array(
      StructField("node", StringType),
      StructField("_documentType", StringType),
      StructField("_plugin", StringType),
      StructField("pid", StringType),
      StructField("host", StringType),
      StructField("level", StringType),
      StructField("ident", StringType),
      StructField("time", TimestampType),
      StructField("_tag_appName", StringType),
      StructField("message", StringType)
      ))

    val syslogLinuxSchema = StructType(Array(
      StructField("node", StringType),
      StructField("_documentType", StringType),
      StructField("_plugin", StringType),
      StructField("pid", StringType),
      StructField("host", StringType),
      StructField("level", StringType),
      StructField("ident", StringType),
      StructField("time", TimestampType),
      StructField("_tag_appName", StringType),
      StructField("message", StringType)
      ))

    val msgSchema = StructType(Array(
      StructField("_documentType", StringType),
      StructField("_plugin", StringType),
      StructField("message", StringType)
      ))

    //Process parition strategy json
    val pJson = JSON.parseFull(config.partitionJson)
    println("<<<<<<<<<<<<<<<<<<PartitionJson>>>>>>>>>>>>>>>>>>>")
    println(pJson)
    val partitionStrategyMap:Map[String,Any] = pJson.get.asInstanceOf[Map[String, Any]]
    val _pStrategy = partitionStrategyMap.get("partitionStrategy").get.asInstanceOf[String]
    val _pPivot = partitionStrategyMap.get("partitionPivot").get.asInstanceOf[String]
    val _defaultPartitions = partitionStrategyMap.get("defaultPartitions").get.asInstanceOf[Seq[String]]
    val _pKeys = partitionStrategyMap.get("partitionKeys").get.asInstanceOf[List[Map[String,Any]]]
    var pMap:Map[String, Seq[String]] = Map()
    for (pkey <- _pKeys){
      pMap += (pkey("pivotValue").asInstanceOf[String] -> pkey("keys").asInstanceOf[Seq[String]])
    }

    schemaMap += ("default" -> defaultSchema)
    schemaMap += ("msgSchema" -> msgSchema)
    schemaMap += ("syslog.linux" -> syslogLinuxSchema)

    partitionMap += ("default" ->
      Seq("_documentType", "_plugin", "year", "month", "day"))
    partitionMap += ("syslog.linux" -> Seq("_documentType", "_plugin"))

    val _msg = df.selectExpr("CAST (value as string) as json")
      .select(from_json($"json", schema=schemaMap("msgSchema")).as("data"))

    val msg = _msg.withColumn("_documentType",(_msg("data._documentType")))
      .withColumn("_plugin",(_msg("data._plugin")))
      .withColumn("_message",(_msg("data.message"))).drop(_msg("data"))

    _msg.printSchema

    val columns = Seq("_documentType","_plugin")

    msg.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.createOrReplaceGlobalTempView("batchDFView")
    println("*********************Starting foreachBatch********************")

    var query = ""
    var first = 1

    for (key <- schemaKeys){
      if (first == 1){
        first = 0;
        query = query + " " + key + " "
      }
      else {
        query = query + ", " + key + " "
      }
    }

    if (first == 1){
      println("No schema coluld be determined")
    }
    else {
      query = "SELECT " + query + " FROM global_temp.batchDFView"
      println("*****************query to execute*******************")
      println(query)
      //Get the dataframe with 2 columns col(_documentType) col(_plugin)
      val schemaDF = spark.sql(query)
      //Find distinct rows from schemaDF
      val schemaUniqueDF = schemaDF.dropDuplicates;
      //Get the tuple (_documentType, _plugin)
      val schemaUniqueKeyTuple = schemaUniqueDF.rdd.map(x=>x).collect;
      val partitionValueTuple = schemaUniqueDF.select(_pPivot).collect
      //Declare an array of dataframes
      //var arrayDF = new Array[DataFrame](schemaUniqueKeyTuple.length)
      //var arrayDF = DataFrame()
      //Key to schemaMap
      var schemaId = ""
      var oindex = 0

      for (item <- schemaUniqueKeyTuple){
        first = 1
        query = ""
        schemaId = ""
        var msgPPivotValue = None 
        for (index <- 0 until item.length) {
          if (first == 1){
            first = 0;
            query = query + schemaKeys(index) + "=" + "'" + item(index) + "'";
            //build key to schemaMap. Example: syslog.linux
            schemaId += item(index)
          }
          else {
            query = query + " AND " + schemaKeys(index) +
              "=" + "'" + item(index) + "'";
            schemaId += "." + item(index)
          }
        }
        query = "SELECT * FROM global_temp.batchDFView WHERE " + query
        println("*************query to execute to build DFs***************")
        println(query)
        var arrayDF = spark.sql(query)
        val op = "gs://biodock-bucket/output-foreach"
        //arrayDF.show(false)
        //Data frame to hold internal message/Actual Log
        //or Metric without any other column. root/data/<.message>
        var _payloadDF = arrayDF.selectExpr("CAST (_message as string) as json")
          .select(from_json($"json",
            schema=schemaMap.getOrElse(schemaId, schemaMap("default"))).as("data"))
        /*
        //Build schema URL based on schemaID
        var logSchemaSource = schemaSource + schemaId + "/versions/latest"
        //Get schema from registry
        var logSchemaResp = requests.get(logSchemaSource)
        println(">>>>>>>>>>>>>>Schema returned by registry>>>>>>>>>>>>>>>>>")
        println(logSchemaSource)
        println(logSchemaResp.text)
        val logSchemaJson = Json.parse(logSchemaResp.text)
        val schemaStr = logSchemaJson("schema")
        var plainAvroSchema = schemaStr.toString.replace("\\","").stripPrefix("\"").stripSuffix("\"")
        var avroSchema = AvroSchemaUtils.parse(plainAvroSchema)
        var logSchema = SparkAvroConversions.toSqlType(avroSchema)

        var _payloadDF = arrayDF.selectExpr("CAST (_message as string) as json")
          .select(from_json($"json",
            schema=logSchema).as("data"))
        */
        var payloadDF = _payloadDF.withColumn("_time", _payloadDF("data.time").cast("timestamp"))
        //Data frame to hold internal message/Actual Log
        //or Metric with other columns requred to partition data.
        //-root/data/message | -root/data/_documentType | -root/data/_plugin
        var augmentPayloadDF = payloadDF.withColumn("_documentType", payloadDF("data._documentType"))
            .withColumn("_plugin", payloadDF("data._plugin"))
            .withColumn("year", year(payloadDF("_time")))
            .withColumn("month", month(payloadDF("_time")))
            .withColumn("day", dayofmonth(payloadDF("_time")))

        var _partitionBy = _defaultPartitions 
        if (_pStrategy == "variable"){
          _partitionBy = pMap.getOrElse(partitionValueTuple(oindex)(0).asInstanceOf[String], _defaultPartitions) 
        }
        oindex = oindex + 1

        augmentPayloadDF.printSchema
        augmentPayloadDF.show(false)
        augmentPayloadDF.write
          .mode("append")
          .format("orc")
          .partitionBy(_partitionBy:_*)
          .save(op)
        }
      }
    }.trigger(ProcessingTime("1 minutes"))
      .option("checkpointLocation", "gs://biodock-bucket/checkpoint-foreach")
      .start
      .awaitTermination
  }
}
