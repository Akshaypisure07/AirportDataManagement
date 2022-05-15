from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark= SparkSession.builder.appName("airportdatamgmt").master("local[*]").getOrCreate()

    routesdf= spark.read.parquet(r"D:\Akshay\pyspark\routes.snappy.parquet")
    routesdf.printSchema()
    routesdf.show()

    routesdf.write.csv(r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\inputfiles\routes")