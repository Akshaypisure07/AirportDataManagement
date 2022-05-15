from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark= SparkSession.builder.appName("airportdatamgmt").master("local[*]").getOrCreate()

    planedf=spark.read.csv(r"D:\Akshay\pyspark\plane.csv",inferSchema=True,header=True,sep="")
    planedf.show()
    planedf.printSchema()

    planedf.write.csv(r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\inputfiles\plane")