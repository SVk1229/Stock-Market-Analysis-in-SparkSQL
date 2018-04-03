package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.DataFrame

object stockMarketAnalysis {
  case class company(date:String, open:Double, high:Double, low:Double, close:Double, volume:Int, adjclose:Double)
  
  def split(line:String): company= {
    
    val a = line.split(",")
    
    company(a(0),a(1).toDouble, a(2).toDouble,a(3).toDouble,a(4).toDouble,a(5).toInt,a(6).toDouble)
   // company(a(0),a(1).toFloat, a(2).toFloat,a(3).toFloat,a(4).toFloat,a(5).toInt,a(6).toFloat)
 }
  
 
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
   
   val spark = SparkSession
               .builder()
               .appName("WordCountSQL")
               .master("local[*]")
               .getOrCreate()
   
   /* val aaondf = spark.read.format("csv")
                      .option("inferSchema", "true")
                      .option("header", "true")
                      .load("file:///c:/SparkScala/DS/AAON.csv")
 
     */
               val aaondf = spark.sparkContext.textFile("file:///c:/SparkScala/stock/AAON.csv")
               val abaxdf = spark.sparkContext.textFile("file:///c:/SparkScala/stock/ABAX.csv")
               val fastdf = spark.sparkContext.textFile("file:///c:/SparkScala/stock/FAST.csv")
               val ffivdf = spark.sparkContext.textFile("file:///c:/SparkScala/stock/FFIV.csv")
               val msftdf = spark.sparkContext.textFile("file:///c:/SparkScala/stock/MSFT.csv")
               
               val df1 = aaondf.map(split)
               val df2 = abaxdf.map(split)
               val df3 = fastdf.map(split)
               val df4 = ffivdf.map(split)
               val df5 = msftdf.map(split)
 
       import spark.implicits._
                
       val aaondf1 = df1.toDS.cache()
       val abaxdf1 = df2.toDS .cache()
       val fastdf1 = df3.toDS .cache()
       val ffivdf1 = df4.toDS .cache()
       val msftdf1 = df5.toDS .cache()
      
       aaondf1.createOrReplaceTempView("aaon")
       abaxdf1.createOrReplaceTempView("abax")
       fastdf1.createOrReplaceTempView("fast")
       ffivdf1.createOrReplaceTempView("ffiv")
       msftdf1.createOrReplaceTempView("msft")
       
      aaondf1.printSchema()
  
  // val cp  = spark.sql("select year(date) as year, avg(aaon.close) as aaonavg from aaon group by year").show()
   
     //  PS1: compute the closing price 
     
  val avgClosingPrice_year = spark.sql("""select year(aaon.date) as year, avg(aaon.close) as aaonavg, avg(abax.close) as abaxavg, avg(fast.close) as fastavg, avg(ffiv.close) as fivavg,
avg(msft.close) as msftavg from aaon join abax on aaon.date = abax.date join fast on aaon.date = fast.date join ffiv on aaon.date = ffiv.date join msft on aaon.date = msft.date group by year order by year """).show()
 
// PS2:  companies with highest closing prices
   
  val highestClosingPrice = spark.sql("""select year(aaon.date) as year, max(aaon.close) as aaonmax, max(abax.close) as abaxmax, max(fast.close) as fastmax, max(ffiv.close) as ffivmax,
 max(msft.close) as msftmax from aaon join abax on aaon.date = abax.date join fast on aaon.date = fast.date join ffiv on aaon.date = ffiv.date join msft on aaon.date = msft.date group by year order by year""").show()
 
 //PS3: avg closing per month
 
 val avgClosingPrice_month = spark.sql("""select month(aaon.date) as month, avg(aaon.close) as aaonavg, avg(abax.close) as abaxavg, avg(fast.close) as fastavg, avg(ffiv.close) as fivavg,
 avg(msft.close) as msftavg from aaon join abax on aaon.date = abax.date join fast on aaon.date = fast.date join ffiv on aaon.date = ffiv.date join msft on aaon.date = msft.date group by month order by month""").show()
 
 //PS4: number of big price rises and losses
 
  val Price_Rise_Loss = spark.sql("""select year(aaon.date) as year, max(aaon.high) as aaonhigh,max(aaon.low) as aaonlow,max(abax.high) as abaxhigh,max(abax.low) as abaxlow,max(fast.high) as fasthigh,max(fast.low) as fastlow,max(ffiv.high) as fivhigh,
  max(ffiv.low) as ffivlow,max(msft.high) as msfthigh,max(msft.low) as msftlow from aaon join abax on aaon.date = abax.date join fast on aaon.date = fast.date join ffiv on aaon.date = ffiv.date join msft on aaon.date = msft.date group by year order by year""").show()
    
  }
      
      
      
      
}