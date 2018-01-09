import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

// Parse input params
val sconf = new SparkConf()
val paramsString = sconf.get("spark.driver.extraJavaOptions")
val paramsArray = paramsString.slice(2,paramsString.length).split(",")

val arg1 = paramsArray(0)
val path_unzipped = paramsArray(1)
val path_temp = paramsArray(2)

// Create RDD from CSV file
// RDD for mobility data
val mobil = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\t").load(path_unzipped + arg1)
mobil.registerTempTable("mobils")

// RDD for airport admins data (load airport_admins.csv file from current working directory)
val airport_admins = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("./airport_admins.csv")
airport_admins.registerTempTable("airport_admins")

// SQL
val statement = "select m.year, m.week, sum(m.traffic_estimation) as cnt, first_value(a1.id_1) as origin_admin_1, first_value(a2.id_1) as destination_admin_1, first_value(m.origin_airport) as origin_airport, first_value(m.destination_airport) as destination_airport from mobils m inner join airport_admins a1 on a1.iata_code = m.origin_airport inner join airport_admins a2 on a2.iata_code = m.destination_airport group by a1.id_1, a2.id_1, year, week order by week"
println(statement)

// Runs query
val distOrigins = sqlContext.sql(statement)

// Filter and display airports IATA codes that are not in the airport_admins.csv file
val missing_origin_airports = distOrigins.filter("origin_admin_1 is null").select("origin_airport")
val missing_destination_airports = distOrigins.filter("destination_admin_1 is null").select("destination_airport")

missing_origin_airports.unionAll(missing_destination_airports).distinct().collect().foreach(println)

// Outputs contents to specfied path
distOrigins.write.format("com.databricks.spark.csv").save(path_temp + arg1)
System.exit(0)
