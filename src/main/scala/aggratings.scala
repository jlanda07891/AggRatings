import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, FloatType, DoubleType, StringType, StructField, StructType, DataType, DateType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql

object AggRatingsExporter {

	def main(args:Array[String]) = {
		
		val spark = SparkSession
			.builder()
			.master("local")
			.appName("SimpleApp")
			.getOrCreate

		import spark.implicits._

		val arglist = args.toList
            	val file_p  = arglist(0)
	
		// the schema to apply on the dataframe	
		val schema = new StructType()
              		.add("userId"	, StringType, true)
              		.add("itemId"	, StringType, true)
              		.add("rating"	, FloatType,  true)
              		.add("timestamp", IntegerType,true)

		val df_csv = spark
			.read
			.format("csv")
			//.option("header", "true")
			//.schema(schema)
			.load(s"./src_file/${file_p}")

		// rename columns with headers
		val df_base = df_csv
			.withColumnRenamed("_c0","userId")
			.withColumnRenamed("_c1","itemId")
			.withColumnRenamed("_c2","rating")
			.withColumnRenamed("_c3","timestamp")

		// assign a unique identifier (consecutive integers) to each userId	
		val df_userid = Utils.Reduce.init_unique_userId(spark, df_base)
		// assign a unique identifier (consecutive integers) to each itemId	
		val df_itemid = Utils.Reduce.init_unique_itemId(spark, df_base)
		
		// joins the base dataframe on df_userid + df_itemid to add "itemIdAsInteger" and "userIdAsInteger" columns
		val joined_user_item = df_base
			.join(df_userid, df_base("userId") === df_userid("us_Id"), "inner")
			.join(df_itemid, df_base("itemId") === df_itemid("it_Id"), "inner")
			.cache
		
		// gets today's date as "yyyy-MM-dd" format 
		val date_today = Utils.Date.today()

		/** 
		* Apply a penalty of 5% on ratingSum for each day of gap with the max timestamp of itemId + userId
		*/
		val penalty_rate = 0.95
		def apply_penalty = udf {(nb_days:Int,rating:Float) => {
			rating * Math.pow(penalty_rate, nb_days)
		}}


		val df_aggratings = joined_user_item
			.select(
				"userIdAsInteger", 
				"itemIdAsInteger", 
				"rating", 
				"timestamp"
			)
			// unique combinations of userId + itemId
			.groupBy(
				"userIdAsInteger", "itemIdAsInteger"
			)
			.agg(
				max("timestamp") as "max_timestamp",
				sum("rating") as "ratingSum"
			)
			// get today's date
			.withColumn("now_date"		 , lit(date_today).cast("date") )
			// converts unix_timestamp (in milliseconds) to timestamps in seconds, then converts to date
			.withColumn("max_timestamp_date" , ($"max_timestamp"/1000).cast("timestamp").cast("date") )
			// calculate the gap of days between the dates
			.withColumn("diff_days_timestamp", datediff($"now_date", $"max_timestamp_date") )
			// apply a penalty of 5% on ratingSum for each day of gap with the max timestamp of itemId + userId 
			.withColumn("ratingSumPenalty", when($"diff_days_timestamp" > 0, apply_penalty($"diff_days_timestamp",$"ratingSum")).otherwise(0) )
			// only keep ratings > 0.01
			.filter("ratingSumPenalty > 0.01")
			.orderBy(
				col("ratingSum").desc
			)

		//df_aggratings.printSchema
		df_aggratings.show(20, false);

		Utils.Export.save(df_aggratings,"aggratings")
		Utils.Export.save(df_userid,"lookup_user")
		Utils.Export.save(df_itemid,"lookup_product")
	}
}
