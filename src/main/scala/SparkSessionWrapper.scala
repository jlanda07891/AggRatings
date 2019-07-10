import org.apache.spark.sql.SparkSession

/**
* every class inheriting from this trait can access to the session with the "spark" variable
* usefull because the SparkSession is expensive and the app will run faster if we only create one SparkSession
*/
trait SparkSessionWrapper {
  
	lazy val spark: SparkSession = {
		SparkSession
			.builder()
			.master("local")
			.appName("AggRatingsApp")
			.getOrCreate

	  }
}
