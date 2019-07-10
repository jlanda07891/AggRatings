import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, Column}

object Utils extends Serializable {
 
    object Date {
        
        val dateFmt = "yyyy-MM-dd"

	/** 
	* Provides a today's date using dateFmt format
	*/
	def today(): String = {
		val date 	= new Date()
		val date_format = new SimpleDateFormat(dateFmt) 
		date_format.format(date)
	}

    }

    object Reduce {

	/** 
	* Provides a dataframe of users with consecutive integers as unique ID	
	*/
	def init_unique_userId(spark:SparkSession, df:Dataset[Row]) = {

		import spark.implicits._

		// performs a sort calculation over a group of users
		val byUserId = Window.orderBy($"count_user_id".desc)

		// sorts users by count desc 
		// creates sequential number as rank ("userIdAsInteger")
		df
			.select("userId")
			.groupBy("userId")
			.agg(sum(lit(1)) as "count_user_id")
			.withColumn("userIdAsInteger", row_number().over(byUserId))
			.withColumn("userIdAsInteger" , $"userIdAsInteger" - 1)
			.withColumnRenamed("userId","us_Id")
			.cache		
	}

	/** 
	* Provides a dataframe of items with consecutive integers as unique ID	
	*/
	def init_unique_itemId(spark:SparkSession, df:Dataset[Row]) = {
		
		import spark.implicits._

		// performs a sort calculation over a group of items
		val byItemId = Window.orderBy($"count_item_id".desc)

		// sorts items by count desc
                // creates sequential number as rank ("itemIdAsInteger")
		df
			.select("itemId")
			.groupBy("itemId")
			.agg(sum(lit(1)) as "count_item_id")
			.withColumn("itemIdAsInteger", row_number().over(byItemId))
			.withColumn("itemIdAsInteger" , $"itemIdAsInteger" - 1)
			.withColumnRenamed("itemId","it_Id")
			.cache		
	}

    }

    object Export {

	val path_dir = "./exports/"

	/**
	* export a "filename" csv file from a dataframe
	*/
	def save(report:Dataset[Row], filename:String) = {
		report
			// combines all partitions to one
			.coalesce(1)
			.write
			.option("header", "true")
			.mode("overwrite")
			.csv(s"${path_dir}${filename}")
 	}

    } 

}

