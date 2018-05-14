import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

// Runs query on the dataset aggregating by the given admin level
// This function makes the assumption that exists a DataFrame with name "mobils" with Amadeus data
def aggregateDataByAdminLevel( sqlContext : SQLContext, aggregation_level : String ) : DataFrame = {
  // Maps aggregation level to field name
  val aggregations = Map( "admin0" -> "id_0", "admin1" -> "id_1", "admin2" -> "id_2", "admin3" -> "id_3", "admin4" -> "id_4", "admin5" -> "id_5" )
  var selected_aggregation_level = ""

  // Validate aggregation level input
  try {
    selected_aggregation_level = aggregations( aggregation_level )
  } catch {
    case ex: Exception => {
      println("Invalid argument for aggregation level")
      println(ex)
      System.exit(1)
    }
  }

  // RDD for airport admins data (load airport_admins.csv file from current working directory)
  val airport_admins = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("./airport_admins.csv")
  airport_admins.registerTempTable("airport_admins")

  val statement = "select m.year, m.week, sum(m.traffic_estimation) as cnt, first_value(a1.${aggregation_level}) as origin_admin, first_value(a2.${aggregation_level}) as destination_admin, first_value(m.origin_airport) as origin_airport, first_value(m.destination_airport) as destination_airport from mobils m inner join airport_admins a1 on a1.iata_code = m.origin_airport inner join airport_admins a2 on a2.iata_code = m.destination_airport group by a1.${aggregation_level}, a2.${aggregation_level}, year, week order by week".replace("${aggregation_level}", selected_aggregation_level)
  println(statement)

  // Runs query
  return sqlContext.sql(statement)
}

// Runs query on the dataset aggregating by the given admin0
// This function makes the assumption that exists a DataFrame with name "mobils" with Amadeus data
def aggregateDataByAdmin0( sqlContext : SQLContext ) : DataFrame = {
  val statement = "select year, week, sum(traffic_estimation) as cnt, first_value(origin_country), first_value(destination_country) from mobils group by origin_country, destination_country, year, week order by week"
  // Runs query
  return sqlContext.sql(statement)
}

// Print missing airports data
def printMissingAirportsData( distOrigins : DataFrame ) = {
  // Filter and display airports IATA codes that are not in the airport_admins.csv file
  val missing_origin_airports = distOrigins.filter("origin_admin is null").select("origin_airport")
  val missing_destination_airports = distOrigins.filter("destination_admin is null").select("destination_airport")

  // Print lines
  missing_origin_airports.unionAll(missing_destination_airports).distinct().collect().foreach(println)
}

val sqlContext = new SQLContext(sc)

// Parse input params
val sconf = new SparkConf()
val paramsString = sconf.get("spark.driver.extraJavaOptions")
val paramsArray = paramsString.slice(2, paramsString.length).split(",")

val filename = paramsArray(0)
val aggregation_level = paramsArray(1)
val path_unzipped = paramsArray(2)
val path_temp = paramsArray(3)

// Create RDD from CSV file
// RDD for mobility data
val mobil = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\t").load(path_unzipped + "/" + filename)
mobil.registerTempTable("mobils")

// Run the query

var distOrigins : DataFrame = null

// with admin0 we do not need to load the airport_admins file, we can aggregate using Amadeus country fields
if ( aggregation_level == "admin0" ){
  distOrigins = aggregateDataByAdmin0(sqlContext)
} else {
  // Otherwise, we need to load external resources and perform the join
  distOrigins = aggregateDataByAdminLevel(sqlContext, aggregation_level)
  printMissingAirportsData(distOrigins)
}

// Outputs contents to specfied path
distOrigins.write.format("com.databricks.spark.csv").save(path_temp + filename)
System.exit(0)
