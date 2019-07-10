# AggRatings
A Spark application to aggregate and group data of a big CSV file

# Deploy the application

Install the needed services / dependencies : java, sbt and scala
```
sudo apt install default-jre
sudo apt-get install sbt
sudo apt-get install scala
```

Install Apache Spark. spark-submit and spark-shell commands are located in bin folder :
```
jlanda@DESKTOP-0S8UHRQ:~/spark-2.4.3-bin-hadoop2.7/spark-2.4.3-bin-hadoop2.7/bin$ ll
...
-rwxrwxrwx 1 jlanda jlanda 3122 May  1 07:19 spark-shell*
-rwxrwxrwx 1 jlanda jlanda 1040 May  1 07:19 spark-submit*
...
```

Clone the project and navigate to the project's root folder
```
$ git clone https://github.com/jlanda07891/AggRatings.git
$ cd ./AggRatings
```

Finally, use Sbt to package the application 
```
sbt package
```

# Run the application

First, copy the csv file to parse in ./src_file/ (here xag.csv)
```
jlanda@DESKTOP-0S8UHRQ:~/spark-2.4.3-bin-hadoop2.7/spark-2.4.3-bin-hadoop2.7/bin/AggRatings$ ll src_file/
-rw-rw-rw- 1 jlanda jlanda        71 Jul 10 10:20 .gitignore
-rwxrwxrwx 1 jlanda jlanda 787271850 Jul  8 14:54 xag.csv*
```

Then run the spark-app with spark-submit and specify the name of the csv file as parameter :
```
../spark-submit --master local[*] target/scala-2.11/spark-nightly_2.11-1.0.jar xag.csv
```

# Structure

```
jlanda@DESKTOP-0S8UHRQ:~/spark-2.4.3-bin-hadoop2.7/spark-2.4.3-bin-hadoop2.7/bin/AggRatings$ tree
.
├── build.sbt - The components of Spark used in this project (dataframes / SQL) and the Spark version are specified here
├── exports - repository for output files
├── README.md
├── src - source code of the app with Scala classes
│   └── main
│       └── scala
│           ├── aggratings.scala
│           ├── SparkSessionWrapper.scala
│           └── Utils.scala
├── src_file - repository of the input file
│   └── xag.csv
└── target - the jar file is produced in path: ./target/scala-2.11/spark-nightly_2.11-1.0.jar
    ├── scala-2.11
    │   ├── aggratings_2.11-1.0.jar
    │   ├── classes
    │   │   ├── *.class
    │   └── resolution-cache
    │       ├── ...
```
