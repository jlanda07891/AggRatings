name := "aggratings"

version :="1.0"

scalaVersion := "2.11.8"

// Spark
libraryDependencies ++= Seq(
    // Base spark lib
    "org.apache.spark" %% "spark-core"  % "2.2.0",
    // Support for dataframes / SQL
    "org.apache.spark" %% "spark-sql"   % "2.2.0",
    // Spark AVRO support
    "com.databricks" %% "spark-avro" % "4.0.0"
)
