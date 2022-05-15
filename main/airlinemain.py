from pyspark.sql import SparkSession
from readdatautil import ReadDataUtil
from pyspark.sql.types import *
if __name__ == '__main__':
    spark= SparkSession.builder.appName("airlinedatamanagement").master("local[*]").getOrCreate()
    # rdu= ReadDataUtil()
    dataschema= StructType([StructField("Airline id",IntegerType()),
                            StructField("name",StringType()),
                            StructField("Alias",StringType()),
                            StructField("IATA",StringType()),
                            StructField("ICAO",StringType()),
                            StructField("callsign",StringType()),
                            StructField("country",StringType())])
    # df= rdu.readCsv(spark=spark,schema=dataschema,path=r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\processedfiles\airline.csv")
    airlinedf=spark.read.csv(r"D:\Akshay\pyspark\airline.csv",schema=dataschema)
    airlinedf.show()
    airlinedf.printSchema()

    # airlinedf.write.csv(r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\processedfiles\airline")

    # Q1 In any of your input file if you are getting \N or null values in your column and that column
    # is of string type then put default value as "(unknown)" and if column is of type integer then put -1

    resultdf=airlinedf.replace(["\\N"],["unknown"],["Alias","IATA","ICAO","callsign","country"])\
                      .fillna("unknown",("Alias","IATA","ICAO","callsign","country"))

    resultdf.show()


    #Q2.find the country name which is having both airlines and airport

