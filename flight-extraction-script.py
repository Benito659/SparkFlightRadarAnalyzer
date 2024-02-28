# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, LongType
from FlightRadar24 import FlightRadar24API
from FlightRadar24.errors import CloudflareError
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from shapely.geometry import Point, Polygon
from datetime import datetime
import requests
import time
import os
import shutil


fr_api = FlightRadar24API()
spark = SparkSession.builder.config(conf=SparkConf()).config("spark.master", "local[*]").appName("Data Extraction Phase").getOrCreate()

#  GET LATEST PATH OF PARQUET FILES
def get_all_paths(folder_path):
    all_paths = []
    for foldername, subfolders, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            if file_path.endswith(".parquet"):
                all_paths.append(file_path)
    return all_paths

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
    filename= folderName+str(timestamp)+".parquet"
    return "ExtractionFiles/{}/rawzone/tech_year={}/tech_month={}/tech_day={}/{}-({},{})".format(folderName.capitalize(),years_only,month_and_year,day_month_year,folderName,day_month_year,hour_only),filename

# FONCTION TO EXTRACT DATAS INTO PARQUET
def extractionToParquet(dictionnaireList,pathToWrite,schema):
    df = spark.createDataFrame(dictionnaireList,schema=schema)
    tmp_path=pathToWrite+"/tmp_data"
    current_path = os.getcwd()
    print("Current Working Directory:", current_path)
    df.coalesce(1).write.format("parquet").mode("append").save(tmp_path)


def copy_file(file_path,path , filename,tmp_path):
    import shutil
    destination_path = file_path + "/" + filename
    if os.path.exists(path) and os.path.exists(tmp_path) :
        shutil.copy(path, destination_path)
        shutil.rmtree(tmp_path)
    else:
        print(f"The file {path} does not exist.")


# FONCTION TO REWRTE NAMES OF DATAS INTO PARQUET
def rewriteToParquet(pathToWrite,filenameToReplace):
    tmp_path=pathToWrite+"/tmp_data"
    paths = get_all_paths(pathToWrite)
    for path in paths:
        copy_file(pathToWrite, path, filenameToReplace,tmp_path)


#  AIRLINE EXTRACTION
output_parquet_path_airlines,filename = getOutputPath("airlines")
try:
    airlines = fr_api.get_airlines()
    airlines_schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Code", StringType(), True),
        StructField("ICAO", StringType(), True)
    ])
    extractionToParquet(airlines,output_parquet_path_airlines,airlines_schema)
    rewriteToParquet(output_parquet_path_airlines,filename)
except CloudflareError as cf_error:
    print(f"CloudflareError: {cf_error}")
    time.sleep(2) 
except Exception as e:
    print(f"An unexpected error occurred: {e}")

#  ZONES EXTRACTION
def get_dict_from_zones(key,value):
    dictzone={}
    dictzone["zone"]=key
    dictzone["tl_y"]=value["tl_y"]
    dictzone["tl_x"]=value["tl_x"]
    dictzone["br_x"]=value["br_x"]
    dictzone["br_y"]=value["br_y"]
    return dictzone
try:
    zones = fr_api.get_zones()
    continent_zones,country_zones,city_zones=[],[],[]
    for key, value in zones.items():
        dictzone=get_dict_from_zones(key,value)
        continent_zones.append(dictzone)
        if "subzones" in value :
            for secondkey,countryvalue in value["subzones"].items():
                dictzone=get_dict_from_zones(secondkey,countryvalue)
                dictzone["continent"]=key
                country_zones.append(dictzone)
                if "subzones" in countryvalue :
                    for thirdkey,cityvalue in countryvalue["subzones"].items():
                        dictzone=get_dict_from_zones(thirdkey,cityvalue)
                        dictzone["country"]=secondkey
                        city_zones.append(dictzone)
    continent_schema = StructType([
        StructField("zone", StringType(), True),
        StructField("tl_y", DoubleType(), True),
        StructField("tl_x", DoubleType(), True),
        StructField("br_x", DoubleType(), True),
        StructField("br_y", DoubleType(), True)
    ])
    country_schema = StructType([
        StructField("zone", StringType(), True),
        StructField("tl_y", DoubleType(), True),
        StructField("tl_x", DoubleType(), True),
        StructField("br_x", DoubleType(), True),
        StructField("br_y", DoubleType(), True),
        StructField("continent", StringType(), True)
    ])
    city_schema = StructType([
        StructField("zone", StringType(), True),
        StructField("tl_y", DoubleType(), True),
        StructField("tl_x", DoubleType(), True),
        StructField("br_x", DoubleType(), True),
        StructField("br_y", DoubleType(), True),
        StructField("country", StringType(), True)
    ])
    continent_zones_new = [Row(zone=row['zone'], tl_y=float(row['tl_y']), tl_x=float(row['tl_x']), br_x=float(row['br_x']), br_y=float(row['br_y'])) for row in continent_zones]
    country_zones = [Row(zone=row['zone'], tl_y=float(row['tl_y']), tl_x=float(row['tl_x']), br_x=float(row['br_x']), br_y=float(row['br_y']),continent=row['continent']) for row in country_zones]
    city_zones = [Row(zone=row['zone'], tl_y=float(row['tl_y']), tl_x=float(row['tl_x']), br_x=float(row['br_x']), br_y=float(row['br_y']),country=row['country']) for row in city_zones]
    output_parquet_path_zones_continent,filename_continent = getOutputPath("zctinent")
    output_parquet_path_zones_country,filename_country = getOutputPath("znctry")
    output_parquet_path_zones_city,filename_city = getOutputPath("zncty")
    extractionToParquet(continent_zones_new,output_parquet_path_zones_continent,continent_schema)
    rewriteToParquet(output_parquet_path_zones_continent,filename_continent)
    extractionToParquet(country_zones,output_parquet_path_zones_country,country_schema)
    rewriteToParquet(output_parquet_path_zones_country,filename_country)
    extractionToParquet(city_zones,output_parquet_path_zones_city,city_schema)
    rewriteToParquet(output_parquet_path_zones_city,filename_city)
except CloudflareError as cf_error:
    print(f"CloudflareError: {cf_error}")
    time.sleep(2) 
except Exception as e:
    print(f"An unexpected error occurred: {e}")


#  AIRPORT EXTRACTION
try:
    airports = fr_api.get_airports()
    airports = [{
            'latitude':airport.latitude,
            'longitude': airport.longitude, 
            'altitude': airport.altitude, 
            'name': airport.name, 
            'icao': airport.icao, 
            'iata': airport.iata, 
            'country': airport.country } for airport in airports]
    output_parquet_path_airport,filename_airport = getOutputPath("airport")
    airports_schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("altitude", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("country", StringType(), True),
    ])
    airports = [
        Row(
            latitude=float(row['latitude']), 
            longitude=float(row['longitude']), 
            altitude=int(row['altitude']), 
            name=row['name'], 
            icao=row['icao'],
            iata=row['iata'],
            country=row['country']
            ) for row in airports]
    extractionToParquet(airports,output_parquet_path_airport,airports_schema)
    rewriteToParquet(output_parquet_path_airport,filename_airport)
except CloudflareError as cf_error:
    print(f"CloudflareError: {cf_error}")
    time.sleep(2) 
except Exception as e:
    print(f"An unexpected error occurred: {e}")

#  COUNTRY CITY EXTRACTION
def get_region_for_place(place, bounds):
    place_point = Point(place)
    region_polygon = Polygon([
        (bounds[0], bounds[1]),
        (bounds[2], bounds[1]),
        (bounds[2], bounds[3]),
        (bounds[0], bounds[3]),
    ])
    overlap = region_polygon.intersection(place_point).area
    if region_polygon.contains(place_point):
        return overlap
    return None  
COUNTRY_CITY = {}
maxvalue=0
for airport in airports:
    for continent in continent_zones:
        max_overlap = get_region_for_place((airport["latitude"],airport["longitude"]), [continent["tl_y"],continent["tl_x"],continent["br_y"],continent["br_x"]])
        if max_overlap is not None:
            if (airport["country"] not in COUNTRY_CITY):
                COUNTRY_CITY[airport["country"]]=[continent["zone"],(float(airport["latitude"]),float(airport["longitude"]),int(airport["altitude"]))]

print("COUNTRY_CITY = ",COUNTRY_CITY)
country_city_schema = StructType([
    StructField("zone_country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", IntegerType(), True),
    StructField("continent", StringType(), True)
])
country_city_zones = [Row(zone_country=key, latitude=float(value[1][0]), longitude=float(value[1][1]), altitude=int(value[1][2]),continent=value[0]) for key,value in COUNTRY_CITY.items()]
output_parquet_path_country_city_zones,filename_country_city_zones = getOutputPath("znctyn")
extractionToParquet(country_city_zones,output_parquet_path_country_city_zones,country_city_schema)
rewriteToParquet(output_parquet_path_country_city_zones,filename_country_city_zones)



# #  FLIGHT EXTRACTION
try:
    max_retries,pause_time = 2,3
    flights = fr_api.get_flights() 
    evolution_index=0
    flights_details,active_flights = [],[]
    for flight in flights :
        try:
            time.sleep(pause_time)
            evolution_index= evolution_index+1
            detailflight=fr_api.get_flight_details(flight)
            aircraft_code = getattr(flight, 'code', None)
            origin_airport_iata=getattr(flight, 'origin_airport_iata', None)
            print("detailflight = ",detailflight)
            aircraft_model= detailflight.get("aircraft", {}).get("model", {}).get("text", None)
            flight_duration=detailflight.get("time", {}).get("historical", {}).get("flighttime", None)
            last_position=detailflight.get("trail", [])
            last_position=last_position[-1] if last_position else None
            if aircraft_model:
                aircraft_model = aircraft_model[0]
            else:
                aircraft_model = None
            if origin_airport_iata != '':
                for attempt in range(max_retries):
                    try:
                        actual_distance = flight.get_distance_from(fr_api.get_airport(origin_airport_iata))
                        break 
                    except (requests.exceptions.HTTPError, CloudflareError) as e:
                        if attempt < max_retries - 1 and e.response.status_code == 502:
                            # If a 502 error occurs, retry after a delay
                            print(f"Retrying after 3 seconds (Attempt {attempt + 1}/{max_retries})")
                            time.sleep(pause_time)
                        if attempt== max_retries - 1 and e.response.status_code == 502 :
                            actual_distance=0
                            print(f"Error getting distance: {e}")
                        break
            else:
                actual_distance=None
            normal_flight = {
                'flight_id':flight.id,
                'airline_icao': flight.airline_icao,
                'aircraft_code': aircraft_code, 
                'origin_airport_iata': origin_airport_iata, 
                'departure_time': detailflight.get("time", {}).get("real", {}).get("departure", None),
                'departure_arrival': detailflight.get("time", {}).get("real", {}).get("arrival", None),
                'aircraft_model': aircraft_model,
                'flight_duration' :flight_duration,
                'last_position_latitude': last_position["lat"],
                'last_position_longitude': last_position["lng"],
                'last_position_altitude': last_position["alt"]
            }
            flights_details.append(normal_flight)
            if detailflight.get("status", {}).get("live", False):
                dict_active_flight={
                    'flight_id':flight.id,
                    'latitude': flight.latitude, 
                    'longitude':flight.longitude, 
                    'airline_icao': flight.airline_icao,
                    'origin_airport_iata': origin_airport_iata, 
                    'actual_distance':actual_distance,
                    'destination_airport_iata': flight.destination_airport_iata, 
                    'airline_iata': flight.airline_iata,
                    'aircraft_code': aircraft_code, 
                    'aircraft_model': aircraft_model,
                    'departure_time': detailflight.get("time", {}).get("real", {}).get("departure", None),
                    'departure_arrival': detailflight.get("time", {}).get("real", {}).get("arrival", None),
                    'flight_duration' :flight_duration
                }
                active_flights.append(dict_active_flight)
        except CloudflareError as cf_error:
            print(f"CloudflareError: {cf_error}")
            time.sleep(2) 
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    flights_details_schema = StructType([
        StructField("flight_id", StringType(), True),
        StructField("airline_icao", StringType(), True),
        StructField("aircraft_code", StringType(), True),
        StructField("origin_airport_iata", StringType(), True),
        StructField("departure_time", LongType(), True),
        StructField("departure_arrival", LongType(), True),
        StructField("aircraft_model", StringType(), True),
        StructField("flight_duration", StringType(), True),
        StructField("last_position_latitude", DoubleType(), True),
        StructField("last_position_longitude", DoubleType(), True),
        StructField("last_position_altitude", IntegerType(), True)
    ])
    active_flights_schema = StructType([
        StructField("flight_id", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("airline_icao", StringType(), True),
        StructField("origin_airport_iata", StringType(), True),
        StructField("actual_distance", FloatType(), True),
        StructField("destination_airport_iata", StringType(), True),
        StructField("airline_iata", StringType(), True),
        StructField("aircraft_code", StringType(), True),
        StructField("aircraft_model", StringType(), True),
        StructField("departure_time", LongType(), True),
        StructField("departure_arrival", LongType(), True),
        StructField("flight_duration", StringType(), True)
    ])
    output_parquet_path_normal_flight,filename_flight = getOutputPath("flight")
    output_parquet_path_active_flight,filename_active_flight = getOutputPath("acflight")
    extractionToParquet(flights_details,output_parquet_path_normal_flight,flights_details_schema)
    rewriteToParquet(output_parquet_path_normal_flight,filename_flight)
    extractionToParquet(active_flights,output_parquet_path_active_flight,active_flights_schema)
    rewriteToParquet(output_parquet_path_active_flight,filename_active_flight)
except CloudflareError as cf_error:
    print(f"CloudflareError: {cf_error}")
    time.sleep(2) 
except Exception as e:
    print(f"An unexpected error occurred: {e}")





#  FLIGHT BY COUNTRY EXTRACTION
try:
    list_flight_by_country_cool=[]
    for key,country_city in COUNTRY_CITY.items():
        bounds = fr_api.get_bounds_by_point(country_city[1][0], country_city[1][1],country_city[1][2])
        flights = fr_api.get_flights(bounds = bounds)
        for flight in flights:
            dict_flight_country= {
                'country':key,
                'continent':country_city[0],
                'id_flight':flight.id,
            }
            list_flight_by_country_cool.append(dict_flight_country)
    flight_by_country_schema = StructType([
        StructField("country", StringType(), True),
        StructField("continent", StringType(), True),
        StructField("id_flight", StringType(), True)
    ])
    print("list_flight_by_country_cool == ",list_flight_by_country_cool)
    output_parquet_path_flight_by_country,filename_flight_country = getOutputPath("flctry")
    extractionToParquet(list_flight_by_country_cool,output_parquet_path_flight_by_country,flight_by_country_schema)
    rewriteToParquet(output_parquet_path_flight_by_country,filename_flight_country)
except CloudflareError as cf_error:
    print(f"CloudflareError: {cf_error}")
    time.sleep(1)  
except Exception as e:
    print(f"An unexpected error occurred: {e}")