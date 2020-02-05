import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


import com.google.cloud.storage.Bucket
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageOptions

import scala.collection.JavaConverters._



object readORC {
    def main(args: Array[String]) {
        //val sc = new SparkContext()
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
        //sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
        val startTime = args(0).toLong
        val endTime = args(1).toLong
        val tagAppName = args(2)
        val plugin = args(3)
        val logLevel = args(4)
        val docType = args(5)
        val output = args(6)
        val input = args(7) 
        val dirString = args(8)

        var keyword = List[String]()
        var keyword1 = ""

        if (args.length == 10){
            keyword = args(9).split('|').toList
            keyword1 = args(9)
        }

        val resultDir = output + "/result"
        val statDir = output + "/stats"
        val metaDir = output + "/meta"

        val startYear = Integer.parseInt(new SimpleDateFormat("yyyy").format(new Date(startTime * 1000L)));
        val endYear = Integer.parseInt(new SimpleDateFormat("yyyy").format(new Date(endTime * 1000L)));
        val startMonth = Integer.parseInt(new SimpleDateFormat("MM").format(new Date(startTime * 1000L)));
        val endMonth = Integer.parseInt(new SimpleDateFormat("MM").format(new Date(endTime * 1000L)));
        val startDay = Integer.parseInt(new SimpleDateFormat("dd").format(new Date(startTime * 1000L)));
        val endDay = Integer.parseInt(new SimpleDateFormat("dd").format(new Date(endTime * 1000L)));
        val startHour = Integer.parseInt(new SimpleDateFormat("hh").format(new Date(startTime * 1000L))); 
        val endHour = Integer.parseInt(new SimpleDateFormat("hh").format(new Date(endTime * 1000L))); 
        val spark = SparkSession.builder.getOrCreate()

        println("------------------args0------------------")
        println(args(0))
        println("------------------args1------------------")
        println(args(1))
        println("------------------args2------------------")
        println(args(2))
        println("------------------args3------------------")
        println(args(3))
        println("------------------args4------------------")
        println(args(4))
        println("------------------args5------------------")
        println(args(5))
        println("------------------args6------------------")
        println(args(6))
        println("------------------args7------------------")
        println(args(7))
        println("------------------args8------------------")
        println(args(8))

        import spark.implicits._

        val metaAcc = spark.sparkContext.collectionAccumulator[Seq[Row]]("name")
        val log = spark.sqlContext.read.format("orc").load(input)

        log.registerTempTable("log")

        //val query = "SELECT * from log WHERE " + "(day >= " + startDay + " AND hour >= " + startHour + ")" + " OR " + "(day <= " + endDay + " AND hour <= " + endHour + ")"
        //val query = "SELECT data from log WHERE " + "(year = " + startYear + " )" + " AND " + "( month = " + startMonth + " )" + " AND " + "(day >= " + startDay + " AND " + "day <= " + endDay + ")"
        //val query = "SELECT data FROM log WHERE " + "(year >= " + startYear + " AND " + "month >= " + startMonth + " AND " + "day >= " + startDay + " )" + " OR " + "(year <= " + endYear + " AND " + "month <= " + endMonth + " AND " + "day <= " + endDay + " )"  

        var mflag = 0

        var query = ""
        if (startYear == endYear) {
            query = query + "(" + "year==" + startYear
            if (startMonth == endMonth) {
                query =  query + " AND " + "month=" + startMonth
                if (startDay == endDay) {
                    query = query + " AND " + "day=" + startDay + ")"
                }
                else {
                    query = query + " AND " + "(" + "day>=" + startDay + " AND " + "day<=" + endDay + ")" + ")"
                }
            }
            else {
                for (m <- startMonth + 1 to endMonth - 1) {
                    if (mflag == 0) {
                        query = query + ")" + " AND " + "(" +  "month=" + m
                        mflag =  mflag + 1
                    }
                    else {
                        query = query + " OR " + "month=" + m
                        mflag =  mflag + 1
                    }
                }
                if (mflag > 0) {
                    query = query + " OR "
                    mflag = 0
                }
                query = query + "(month=" + startMonth + " AND " + "day>=" + startDay + ")" + " OR " + "(month=" + endMonth + " AND " + "day<=" + endDay + ")"
                query = query + ")"
                
            }
        }
        if (startYear != endYear) {
            for (y <- startYear + 1 to endYear - 1) {
                if (mflag == 0) {
                    query = query + "(" +  "year=" + y
                    mflag = mflag + 1
                }
                else {
                    query = query + " OR " + "year=" + y
                    mflag = mflag + 1
                }
            }
            if (mflag > 0) {
                query = query + ")" + " OR "
                mflag = 0
            }
            query = query + "((year=" + startYear + ")" + " AND "
            for (m <- startMonth + 1 to 12) {
                if (mflag == 0) {
                        query = query + "(" +  "month=" + m
                        mflag = mflag + 1
                    }
                    else {
                        query = query + " OR " + "month=" + m
                        mflag = mflag + 1
                    }
            }
            if (mflag > 0) {
                query = query + ")" + " OR "
                mflag = 0
            }

            query = query + "(month=" + startMonth + " AND " + "day>=" + startDay + "))" + " OR " 
            query = query + "((year=" + endYear + ")" + " AND "

            for (m <- 1 to endMonth - 1) {
                if (mflag == 0) {
                    query = query + "(" +  "month=" + m
                    mflag = mflag + 1
                }
                else {
                    query = query + " OR " + "month=" + m
                    mflag = mflag + 1
                }
            }
            if (mflag > 0) {
                query = query + ")" + " OR "
                mflag = 0
            }
            query = query + "(month=" + endMonth + " AND " + "day<=" + endDay + "))"  
        }

        //if (docType != "*"){
        //    query =  "((" + query + ")" + " AND " + "(_documentType=" + docType + "))"
        //}

        query = "SELECT data FROM log WHERE " + query

        println("Query to execute:")
        println(query)

        var result = spark.sqlContext.sql(query)

        if (logLevel != "*"){
            result = result.filter(result("data.level")===logLevel.toLowerCase())
        }

        /* 
        if (!keyword.isEmpty) {
            def filterColumn (s: String) : Boolean = {
                val mapList = keyword.map(word => s.contains(word))
                mapList.exists(value => value == true)
            }

            val filterUDF = udf(filterColumn _)

            result = result.filter(filterUDF(result("data.message")))
        }
        */

        if (keyword1 != ""){
            result = result.filter($"data.message".rlike(keyword1))
        }

        val totalLogItems = result.count()

        if(totalLogItems > 1000000) {
            val partitionsRequired = totalLogItems/1000000
            spark.conf.set("spark.sql.shuffle.partitions", partitionsRequired)
        } else {
            spark.conf.set("spark.sql.shuffle.partitions", 1)
        }

        val finalResult = result.select("data.*").withColumn("timestamp", $"time".cast("long")).orderBy($"timestamp") 
        val totalLogItemsDF = Seq(totalLogItems).toDF()
        totalLogItemsDF.coalesce(1).write.mode("overwrite").options(Map("compression"->"None")).format("json").save(statDir)
        finalResult.write.format("orc").save(resultDir)


        if(totalLogItems >= 1) {
            
            val bucketName = "biodock-bucket"
            val storage = StorageOptions.getDefaultInstance().getService()
            val blobList = storage.list(bucketName, BlobListOption.prefix(resultDir.stripPrefix("gs://%s/".format(bucketName))))
            println(blobList)
            val filesList = blobList.getValues.asScala
            var partFiles = new ListBuffer[String]()

            for(file <- filesList) {
                val fileName = file.getName
                  println(fileName)
                  if(fileName.contains("part") && fileName.contains("orc")){
                            partFiles += "gs://%s/%s".format(bucketName, fileName)
                                }
            }

            
            println(partFiles.mkString(", "))

            def genMeta(file: String) = future {
                println(file)
                var df = spark.read.format("orc").load(file)
                var totalCount = df.count().toInt
                var startTimestamp = df.first().toSeq.last.toString.toInt
                var endTimestamp = df.agg(max("timestamp")).collect().toSeq.last.get(0).toString.toInt
                var stat = Row(file, totalCount, startTimestamp, endTimestamp)
                metaAcc.add(Seq(stat))
                println(totalCount)
            }

            val metaResult = Future.sequence(
                partFiles.map({case(file) => genMeta(file)})
            )

            while(!metaResult.isCompleted) {
                Thread.sleep(1000)
            }

            val metaSeq = metaAcc.value.asScala.flatten

            val metaSchema = StructType(List(StructField("partName", StringType, true),
                                             StructField("count", IntegerType, true),
                                             StructField("startTimestamp", IntegerType, true),
                                             StructField("endTimestamp", IntegerType, true)))

            val metaDF = spark.createDataFrame(spark.sparkContext.parallelize(metaSeq), metaSchema)
            metaDF.show()
            

            metaDF.coalesce(1).write.mode("overwrite").options(Map("compression"->"None")).format("json").save(metaDir)

        }

        spark.stop()
    }
}
