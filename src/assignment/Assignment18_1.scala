/*
 * 
 * 1) What is the distribution of the total number of air-travelers per year
2) What is the total air distance covered by each user per year
3) Which user has travelled the largest distance till date
4) What is the most preferred destination for all users.
 * 
 * 
 * 
 */


package assignment

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext



object Assignment18_1 {
  def main(args: Array[String]): Unit = {
    //specify the configuration for the spark application using instance of SparkConf
    val config = new SparkConf().setAppName("Assignment 18.1").setMaster("local")
    
    //setting the configuration and creating an instance of SparkContext 
    val sc = new SparkContext(config)
    
    //Entry point of our sqlContext
    val sqlContext = new HiveContext(sc)
    
    //to use toDF method 
    import sqlContext.implicits._
    
    /*
     * create schema for holiday dataset by using case class StructType which contains a sequence of StructField which also a case
     * class, thorugh which we can specify the column name and data type and whether it nullable or not
     */
    val holidays_schema = StructType(List (
    StructField("cID",IntegerType,true),
    StructField("source",StringType,false),
    StructField("destination",StringType,false),
    StructField("mode",StringType,false),
    StructField("distance",IntegerType,false),
    StructField("year",IntegerType,false )
    ))
    
    /*
     * create schema for transport dataset by using case class StructType which contains a sequence of StructField which also a case
     * class, thorugh which we can specify the column name and data type and whether it nullable or not
     */
    
    val transport_schema = StructType(List(
    StructField("name",StringType,false),
    StructField("Min_Fare",IntegerType,false)
    ))
    
    /*
     * create schema for user dataset by using case class StructType which contains a sequence of StructField which also a case
     * class, thorugh which we can specify the column name and data type and whether it nullable or not
     */
    
    val user_schema = StructType(List(
    StructField("id",IntegerType,false),
    StructField("Name",StringType,false),
     StructField("age",IntegerType,false)
    ))
    
    // Create RDD from files stored locally using sc.textFile method
    val holiday_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/holidays")
    val transport_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/transport")
    val user_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/userDetails")
    
    /*
     * Map the columns of dataset to Row case class which will be required to create the dataframe
     */
    val holidays_rowsRDD =holiday_file.map{lines => lines.split(",")}.map{col => Row(col(0).toInt,col(1),col(2),col(3),col(4).toInt,col(5).trim.toInt)}
    
    val transport_rowsRDD = transport_file.map{lines => lines.split(",")}.map{col => Row(col(0),col(1).trim.toInt)}
    
    val user_rowsRDD = user_file.map{lines => lines.split(",")}.map{col => Row(col(0).toInt,col(1),col(2).trim.toInt)}
    
    /*
     * create dataframes using createDataFrame method which takes first parmeter as RDD of Rows and second
     * parameter as schema
     */
    val holidayDF = sqlContext.createDataFrame(holidays_rowsRDD, holidays_schema)
    
    val transportDF = sqlContext.createDataFrame(transport_rowsRDD, transport_schema)
    
    val userDF = sqlContext.createDataFrame( user_rowsRDD, user_schema)
   
    /*
     * Register the dataframes as temporary table which can be used to query 
     */
    
    holidayDF.registerTempTable("holiday")
    
    transportDF.registerTempTable("transport")
    
    userDF.registerTempTable("user")
    /*
     * 1) What is the distribution of the total number of air-travelers per year
     */
     
    //use the count aggregate method and group by year to get the total no of travellers per year
    val airTravelersPerYear = sqlContext.sql("select  count(distinct cID) as Number_of_travellers, year from holiday group by year")
    
    airTravelersPerYear.show
    
    /*
     *2) What is the total air distance covered by each user per year 
     */
    
    //use cube method to get the generate the possible combination of cID and year and sum the distance and filter it 
    val totalDistancePerUserPerYear = holidayDF.cube($"cID",$"year").sum("distance").filter("cID is not null AND year is not null")
    
    //rename the column and display the dataframe 
    totalDistancePerUserPerYear.withColumnRenamed("sum(distance)","Total air Distance").show(30)
    
    /*
     *3) Which user has travelled the largest distance till date 
     */
    
    
    // dataframe which contains the sum of distance covered by each user
    val sum_distanceDF = sqlContext.sql("select distinct cID,sum(distance) OVER(partition by  cID)  as sum_distance from holiday")
    
    //dataframe which contains the max of distance
    val max_distance_DF = sum_distanceDF.agg(max($"sum_distance"))
    
    //join dataframes to get the dataframe which contains the maximus distance travelled bya user id
    val max_distance_travelledDF = sum_distanceDF.join(max_distance_DF,$"sum_distance"=== $"max(sum_distance)","inner")
    
    //join the dataframe to get the dataframe along with user details
    val user_travelled = max_distance_travelledDF.join(userDF,$"cID"===$"id","inner")
    
    //display the dataframe
    user_travelled.show
    
    
    /*
     *4) What is the most preferred destination for all users. 
     */
    
    
    //dataframe which stores the count per destination
    val totalDestDF = holidayDF.groupBy("destination").count
    //dataframe which stores maxmum times destination travelled 
    val maxDestDF = totalDestDF.agg(max("count"))
    //join dataframe to get most travelled destination
    val preferredDestDF = totalDestDF.join(maxDestDF,$"count"===$"max(count)","inner")
    //select the appropriate columns required and store in dataframe
    val preferredDestinationDF =  preferredDestDF.select($"destination",$"count")
    //display the dataframe
    preferredDestinationDF.show
    
 
    
  }
    
 }
