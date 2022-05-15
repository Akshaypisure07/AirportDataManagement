from pyspark.sql import SparkSession
from pyspark.sql.types import *


if __name__ == '__main__':
    spark=SparkSession.builder.appName("airportdatamgmt").master("local[*]").getOrCreate()

    dataschema=StructType([StructField("Airport ID",IntegerType()),
                           StructField("Name", StringType()),
                           StructField("City", StringType()),
                           StructField("Country", StringType()),
                           StructField("IATA", StringType()),
                           StructField("ICAO", StringType()),
                           StructField("Latitude", IntegerType()),
                           StructField("Longitude", IntegerType()),
                           StructField("Altitude", IntegerType()),
                           StructField("Timezone", TimestampType()),
                           StructField("DST time", TimestampType()),
                           StructField("tz", StringType()),
                           StructField("Type", StringType())])

    airportdf=spark.read.csv(r"D:\Akshay\pyspark\airport.csv",schema=dataschema)
    airportdf.printSchema()
    airportdf.show()

    # airportdf.write.csv(r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\processedfiles\airport")
