import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val sconf = new SparkConf()
val paramsString = sconf.get("spark.driver.extraJavaOptions")
val paramsSlice = paramsString.slice(2,paramsString.length)
val paramsArray = paramsSlice.split(",")
val arg1 = paramsArray(0)
val path_unzipped = paramsArray(1)
val path_processed = paramsArray(2)
val statement = "select year, week, sum(traffic_estimation) as cnt, first_value(origin_country), first_value(destination_country) from mobils group by origin_country, destination_country, year, week order by week"
val mobil = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\t").load(path_unzipped + arg1)
mobil.registerTempTable("mobils")
println(statement)
val distOrigins = sqlContext.sql(statement)
distOrigins.write.format("com.databricks.spark.csv").save(path_processed + arg1)
System.exit(0)
