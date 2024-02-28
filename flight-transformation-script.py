import findspark
findspark.init() 
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import count, desc,mean,lower,split,rank,dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType,DoubleType,IntegerType,FloatType,LongType,TimestampType
from FlightRadar24 import FlightRadar24API
from FlightRadar24.errors import CloudflareError
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import requests
import time
import os

# GET CURRENT DATES TO MAKE NAME OF FOLDER TO SAVES FILES
def getCurrentDates():
    current_datetime = datetime.now()
    years_only = current_datetime.year
    hour_only=current_datetime.hour
    month_and_year = current_datetime.strftime("%B-%Y")
    day_month_year = current_datetime.strftime("%d-%B-%Y")
    timestamp = current_datetime.timestamp()
    return years_only,month_and_year,day_month_year,timestamp,hour_only

# GET THE COMPLETES FOLDER OF THE FILES TO SAVES
def getOutputPath(folderName):
    years_only,month_and_year,day_month_year,timestamp,hour_only=getCurrentDates()
    filename= folderName+str(timestamp)+".csv"
    return "ResultFiles/{}/rawzone/tech_year={}/tech_month={}/tech_day={}/{}-({},{})".format(folderName.capitalize(),years_only,month_and_year,day_month_year,folderName,day_month_year,hour_only),filename


spark = SparkSession.builder.config(conf=SparkConf()).config("spark.master", "local[*]").appName("Data Transformation and Reading Phase").getOrCreate()

def get_all_parquet_paths(folder_path):
    all_paths = []
    for foldername, subfolders, filenames in os.walk("ExtractionFiles\\"+folder_path):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            if file_path.endswith(".parquet"):
                all_paths.append(file_path)
    return all_paths

def get_all_csv_paths(folder_path):
    all_paths = []
    for foldername, subfolders, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            if file_path.endswith(".csv"):
                all_paths.append(file_path)
    return all_paths


def fill_null_value_dataframe(df):
    for column in df.columns:
        if df.schema[column].dataType == DoubleType() or df.schema[column].dataType == FloatType():
            df = df.na.fill(0.0, subset=[column])
        elif df.schema[column].dataType == StringType():
            df = df.na.fill("", subset=[column]) 
        elif df.schema[column].dataType ==  IntegerType() or df.schema[column].dataType == LongType():
            df = df.na.fill(0, subset=[column]) 
    return df


def extractionToCsv(df,pathToWrite):
    print("pathToWrite ext=",pathToWrite,)
    tmp_path=pathToWrite+"/tmp_data"
    df.coalesce(1).write.format("csv").mode("append").save(tmp_path)


def copy_file(file_path,path , filename,tmp_path):
    import shutil
    destination_path = file_path + "/" + filename
    shutil.copy(path, destination_path)
    shutil.rmtree(tmp_path)

# FONCTION TO REWRTE NAMES OF DATAS INTO PARQUET
def rewriteToCsv(pathToWrite,filenameToReplace):
    tmp_path=pathToWrite+"/tmp_data"
    paths = get_all_csv_paths(pathToWrite)
    for path in paths:
        copy_file(pathToWrite, path, filenameToReplace,tmp_path)

airline_path = get_all_parquet_paths("airlines".upper()) [-1]
zone_country_city_path = get_all_parquet_paths("znctyn".upper()) [-1]
airport_path = get_all_parquet_paths("airport".upper()) [-1]
flight_path = get_all_parquet_paths("flight".upper()) [-1]
acflight_path = get_all_parquet_paths("acflight".upper()) [-1]
flctry_path = get_all_parquet_paths("flctry".upper()) [-1]

df_airlines = spark.read.parquet(airline_path)
df_zone_country_city = spark.read.parquet(zone_country_city_path)
df_airport = spark.read.parquet(airport_path)
df_flight = spark.read.parquet(flight_path)
df_acflight = spark.read.parquet(acflight_path)
df_flctry = spark.read.parquet(flctry_path)

taille=10000
# DATA CLEANING:

# drop duplicate value
df_airlines = df_airlines.dropDuplicates()
# Standardaizing column
df_airlines = df_airlines.withColumn("Name", lower(col("name")))
# Fill null values in a text column with an empty string
df_airlines = df_airlines.fillna('', subset=['Code'])


# drop duplicate value
df_zone_country_city = df_zone_country_city.dropDuplicates()
# Standardaizing column
df_zone_country_city = df_zone_country_city.withColumn("zone_country", lower(col("zone_country")))
# Fill null values in a text column with an empty string
df_zone_country_city=fill_null_value_dataframe(df_zone_country_city)



# drop duplicate value
df_airport = df_airport.dropDuplicates()
# Standardaizing column
df_airport = df_airport.withColumn("country", lower(col("country")))
df_airport = df_airport.withColumn("name", lower(col("name")))
# Fill null values in a text column with an empty string
df_airport=fill_null_value_dataframe(df_airport)



# drop duplicate value
df_flight = df_flight.dropDuplicates()
# Standardaizing column
df_flight = df_flight.withColumn("aircraft_model|", lower(col("aircraft_model")))
# filtering columns
df_flight = df_flight.filter(
    (col("airline_icao") != '') &
    (col("departure_time").isNotNull()) &
    ((col("aircraft_model").isNotNull()) | (col("aircraft_model") != '')) &
    (col("origin_airport_iata") != '')
)
# Rename somme columns
df_flight = df_flight.withColumnRenamed("departure_time", "departure_origin")
df_flight = df_flight.withColumn("aircraft_model", lower(col("aircraft_model")))
df_flight=fill_null_value_dataframe(df_flight)


# drop duplicate value
df_acflight = df_acflight.dropDuplicates()
# Standardaizing column
df_acflight = df_acflight.withColumn("aircraft_model", lower(col("aircraft_model")))
# Rename somme columns
df_acflight = df_acflight.withColumnRenamed("departure_time", "departure_origin")
# Fill null values in a text column with an empty string
df_acflight=fill_null_value_dataframe(df_acflight)


# drop duplicate value
df_flctry = df_flctry.dropDuplicates()
df_flctry.show()


## 1 la compagnie avec le plus de vol en cours
joinexpression = df_acflight["airline_icao"] == df_airlines["ICAO"]
df_ac_airline=df_acflight.join(df_airlines,joinexpression,"inner")
flight_counts = df_ac_airline.groupBy("Name").agg(count("*").alias("flight_count"))
flight_counts = flight_counts.orderBy(desc("flight_count"))
max_flight_airline =flight_counts.first()
print("\nla compagnie avec le plus de vol en cours: {}\n".format(max_flight_airline["Name"]))
output_path,filename=getOutputPath("ResultatQuestion1")
extractionToCsv(flight_counts,output_path)
rewriteToCsv(output_path,filename)





## 2 Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
jointype="left_outer"
joinexpression_origin = df_ac_airline["origin_airport_iata"] == df_airport["iata"] 
df_ac_airport_origin=df_ac_airline.join(df_airport.select('iata', 'country') ,on=joinexpression_origin, how=jointype)
df_ac_airport_origin = df_ac_airport_origin.withColumnRenamed('country', 'country_origin').withColumnRenamed('iata', 'iata_origin')
joinexpression_origin = df_ac_airport_origin["country_origin"] == df_zone_country_city["zone_country"] 
df_ac_airport_origin=df_ac_airport_origin.join(df_zone_country_city.select('zone_country', 'continent') ,on=joinexpression_origin, how=jointype)
df_ac_airport_origin = df_ac_airport_origin.withColumnRenamed('zone_country', 'zone_country_origin').withColumnRenamed('continent', 'continent_origin')

joinexpression_origin = df_ac_airport_origin["destination_airport_iata"] == df_airport["iata"] 
df_ac_airport_origin=df_ac_airport_origin.join(df_airport.select('iata', 'country') ,on=joinexpression_origin, how=jointype)
df_ac_airport_origin = df_ac_airport_origin.withColumnRenamed('country', 'country_destination').withColumnRenamed('iata', 'iata_destination')
joinexpression_origin = df_ac_airport_origin["country_destination"] == df_zone_country_city["zone_country"] 
df_ac_airport_origin=df_ac_airport_origin.join(df_zone_country_city.select('zone_country', 'continent') ,on=joinexpression_origin, how=jointype)
df_ac_airport_origin = df_ac_airport_origin.withColumnRenamed('zone_country', 'zone_country_destination').withColumnRenamed('continent', 'continent_destination')

df_ac_airport_origin_first=df_ac_airport_origin.select("Name","continent_origin").where("continent_destination = continent_origin")
df_ac_airport_origin_first_new=df_ac_airport_origin_first.groupBy("Name").agg(count("*").alias("flight_count"))
max_df_ac_airport_origin_first_new=df_ac_airport_origin_first_new.orderBy(col("flight_count").desc()).first()
print("la compagnie avec le + de vols régionaux actifs:")
output_path,filename=getOutputPath("ResultatQuestion2")
max_df_ac_airport_origin_first_new_df = spark.createDataFrame([max_df_ac_airport_origin_first_new])
extractionToCsv(max_df_ac_airport_origin_first_new_df,output_path)
rewriteToCsv(output_path,filename)



## 3 Le vol en cours avec le trajet le plus long:
df_acflight = df_acflight.withColumn("actual_distance", col("actual_distance").cast("integer"))
max_distance_flight = df_acflight.orderBy(col("actual_distance").desc()).first()
print("\nLe vol en cours avec le trajet le plus long:\n")
print(max_distance_flight[0], max_distance_flight[3], max_distance_flight[4], max_distance_flight[10])
output_path,filename=getOutputPath("ResultatQuestion3")
max_distance_flight_df = spark.createDataFrame([max_distance_flight])
extractionToCsv(max_distance_flight_df,output_path)
rewriteToCsv(output_path,filename)





## 4 Pour chaque continent, la longueur de vol moyenne
df_ac_airport = df_flight.join(df_airport, df_flight["origin_airport_iata"] == df_airport["iata"])
df_ac_airport = df_ac_airport.withColumn("country", lower(col("country")))
df_flctry = df_flctry.withColumn("country", lower(col("country")))
final_df = df_ac_airport.join(df_flctry, df_ac_airport["country"] == df_flctry["country"])
df_ac_airport.show(10000)
# df_flctry.show(10000)
# final_df = final_df.filter(col("departure_arrival").isNotNull() & col("departure_origin").isNotNull())
final_df = final_df.withColumn("flight_length", (col("departure_arrival") - col("departure_origin")))
result_df = final_df.groupBy("continent").agg(mean("flight_length").alias("mean_flight_length"))
# Pour chaque continent la longueur de vol moyenne
print("\nPour chaque continent la longueur de vol moyenne:\n")
result_df.show()
output_path,filename=getOutputPath("ResultatQuestion4")
extractionToCsv(result_df,output_path)
rewriteToCsv(output_path,filename)




## 5 question L'entreprise constructeur d'avions avec le plus de vols actifs
df_result = df_acflight.withColumn('aircraft_type', split(df_acflight['aircraft_model'], ' ').getItem(0))
df_count = df_result.groupBy('aircraft_type').count()
df_count = df_count.orderBy(col('count').desc())
df_top5 = df_count.limit(5)
print("\nL'entreprise constructeur d'avions avec le plus de vols actifs:\n")
df_top5.show()
output_path,filename=getOutputPath("ResultatQuestion5")
extractionToCsv(df_top5,output_path)
rewriteToCsv(output_path,filename)



## 6 Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
joinexpression = df_acflight["airline_icao"] == df_airlines["ICAO"]
df_active_airline=df_acflight.join(df_airlines,joinexpression,"inner")
joinexpression = df_active_airline["flight_id"] == df_flctry["id_flight"]
df_compagnie_by_county = df_active_airline.join(df_flctry,joinexpression , "inner")
grouped_df = df_compagnie_by_county.groupBy( "Name","country","aircraft_model").agg(count("flight_id").alias("model_count"))
windowSpec = Window.partitionBy("Name", "country").orderBy(col("model_count").desc())
rankedModels = grouped_df.withColumn("model_rank", dense_rank().over(windowSpec))
top3Models = rankedModels.filter(col("model_rank") <= 3)
print("\nPour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage:\n")
top3Models.show()
output_path,filename=getOutputPath("ResultatQuestion6")
extractionToCsv(top3Models,output_path)
rewriteToCsv(output_path,filename)



## Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?
inbound_flights = ( df_acflight.filter(col("destination_airport_iata").isNotNull())
    .groupBy("destination_airport_iata").agg({"*": "count"}).withColumnRenamed("count(1)", "num_inbound_flights")
    .select("destination_airport_iata", "num_inbound_flights")
)
outbound_flights = ( df_acflight.filter(col("origin_airport_iata").isNotNull())
    .groupBy("origin_airport_iata").agg({"*": "count"}).withColumnRenamed("count(1)", "num_outbound_flights")
    .select("origin_airport_iata", "num_outbound_flights")
)
flight_counts = inbound_flights.join(outbound_flights, inbound_flights["destination_airport_iata"] == outbound_flights["origin_airport_iata"], "outer").na.fill(0)
flight_counts = flight_counts.withColumn("flight_difference",  col("num_inbound_flights") - col("num_outbound_flights"))
max_difference_row = flight_counts.orderBy(col("flight_difference").desc()).first()
max_difference_airport = df_airport.filter(col("iata") == max_difference_row["destination_airport_iata"]).select("name").first()
max_difference_airport_name=max_difference_airport["name"]
print(f" l'aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants {max_difference_airport} ({max_difference_row['destination_airport_iata']}) with a difference of {max_difference_row['flight_difference']} flights.")
output_path,filename=getOutputPath("ResultatQuestion7")
max_difference_airport_df = spark.createDataFrame([max_difference_airport])
extractionToCsv(max_difference_airport_df,output_path)
rewriteToCsv(output_path,filename)


