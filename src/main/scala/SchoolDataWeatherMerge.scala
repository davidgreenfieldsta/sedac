// Databricks notebook source


// COMMAND ----------

display(dbutils.fs.ls("/mnt/sedac-energy/input"))

// COMMAND ----------

//school data
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import spark.implicits._
import org.apache.spark.sql.types._

val schoolData = sc.textFile("/mnt/sedac-energy/input/school.csv")


//make schema
//get header row
val header = schoolData.first()

val fields = header.
                  split(",").
                  map(x => x.replaceAll(" ", "")).
                  map(x => x.replace(":","")).
                  map(fieldName => StructField(fieldName, StringType, true))



val schema = StructType(fields)
//end - make schema

//get all but first row
val dataLists = schoolData.filter(x => x != header).map(_.split(","))

//convert records to rows
val dataRows = dataLists.map( x =>  Row.fromSeq(x))

val schoolDataDF = spark.createDataFrame(dataRows, schema)

schoolDataDF.createOrReplaceTempView("school")
display(schoolDataDF)
//schoolDataDF.printSchema()


// COMMAND ----------

//Weather Data
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import spark.implicits._
import org.apache.spark.sql.types._
import java.text.DateFormat
import java.text.SimpleDateFormat

val weatherData = sc.textFile("/mnt/sedac-energy/input/weather.csv")


//make schema
//get header row
val header = weatherData.first()

val fields = header.
                  split(",").
                  map(x => x.replaceAll(" ", "")).
                  map(x => x.replace(":",""))



val structList = List(StructField("weather_time", TimestampType, false ),
                      StructField("USAF", StringType, true ),
                      StructField("WBAN", StringType, true ),
                      StructField("SPD", StringType, true ),
                      StructField("SKC", StringType, true ),
                      StructField("TEMP", StringType, true ),  
                      StructField("DEWP", StringType, true ))

val schema = StructType(structList)
//end - make schema

//get all but first row
val dataLists = weatherData.filter(x => x != header).map(_.split(","))

//convert records to rows
val format: DateFormat = new SimpleDateFormat("yyyyMMddhhmm")
val dataRows = dataLists. 
                   map(x => {
                     List(new java.sql.Timestamp(format.parse(x(2)).getTime()),x(0),x(1),x(3),x(4),x(5),x(6))
                   }).map( x =>  Row.fromSeq(x))

val weatherDataDF = spark.createDataFrame(dataRows, schema)

weatherDataDF.createOrReplaceTempView("weather")
weatherDataDF.show()

// COMMAND ----------

//school data
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import spark.implicits._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.text.DateFormat
import collection.mutable.Buffer
import java.util.Date
import java.sql.Timestamp


val rawSchoolData = sc.textFile("/mnt/sedac-energy/input/school.csv")
val weatherData = sc.textFile("/mnt/sedac-energy/input/weather.csv")


/*
* Create the school RDD
*/
val rawHeader = rawSchoolData.first()
val headerList = rawHeader.split(",")

val rawSchoolDataList = rawSchoolData.filter(row => row != rawHeader).map(_.split(","))

val format: DateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm")
val convertedSchoolDataList = rawSchoolDataList.flatMap( row => {
  val newRow = Buffer[List[Any]]()
  for(i <- 1 to (headerList.length - 1)){
    val concatTime = row(0) + " " + headerList(i).slice(0,5)
    val myDate = format.parse(concatTime).getTime()
    newRow += List(myDate,row(i))
  }
  newRow.toList
  
})

val schema = StructType(List(StructField("measure_time", LongType,false),
                   StructField("kps", StringType,false)))

val schoolDataDF = spark.createDataFrame(convertedSchoolDataList.map(x =>  Row.fromSeq(x)), schema)

schoolDataDF.createOrReplaceTempView("school_data")
schoolDataDF.show()





// COMMAND ----------

//school data + weather data
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import spark.implicits._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.text.DateFormat
import collection.mutable.Buffer
import java.util.Date
import java.sql.Timestamp
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext


val rawSchoolData = sc.textFile("/mnt/sedac-energy/input/school.csv")

val timeList: Array[Timestamp] = sql("select weather_time from weather order by weather_time desc").collect().map(row => row.getAs[Timestamp](0))

// /*
// * Create the school RDD
// */
val rawHeader = rawSchoolData.first()
val headerList = rawHeader.split(",")

val rawSchoolDataList = rawSchoolData.filter(row => row != rawHeader).map(_.split(","))

val format: DateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm")
case class measure_row( measure_time: Timestamp, kps: String)
val convertedSchoolDataList = rawSchoolDataList.flatMap( row => {
  val newRow = Buffer[measure_row]()
  for(i <- 1 to (headerList.length - 1)){
    val concatTime = row(0) + " " + headerList(i).slice(0,5)
    val myDate = new Timestamp(format.parse(concatTime).getTime())
    newRow += measure_row(myDate,row(i))
  }
  newRow.toList
  
})

case class merged_row( measure_time: Timestamp, weather_time: Timestamp, kps: String)
val mergedDF = convertedSchoolDataList.map(row => {
  val weather_time = timeList.filter(x => x.getTime() <= row.measure_time.getTime()).head
  merged_row(row.measure_time,weather_time,row.kps)
}).toDF


mergedDF.createOrReplaceTempView("merged_data")
val weatherDF = sql("select * from weather")
weatherDF.createOrReplaceTempView("merged_data")

mergedDF.join(weatherDF, mergedDF("weather_time") <=> weatherDF("weather_time"), "left").show()


// COMMAND ----------


