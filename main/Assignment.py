from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark= SparkSession.builder.appName("airlinedatamanagement").master("local[*]").getOrCreate()
    # rdu= ReadDataUtil()
    dataschema1= StructType([StructField("Airline_id",IntegerType()),
                            StructField("name",StringType()),
                            StructField("Alias",StringType()),
                            StructField("IATA",StringType()),
                            StructField("ICAO",StringType()),
                            StructField("callsign",StringType()),
                            StructField("Country",StringType())])
    # df= rdu.readCsv(spark=spark,schema=dataschema,path=r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\processedfiles\airline.csv")
    airlinedf=spark.read.csv(r"D:\Akshay\pyspark\airline.csv",schema=dataschema1)
    # airlinedf.show()
    # airlinedf.groupBy().count().show()
    # airlinedf.printSchema()

    # airlinedf.write.csv(r"C:\Users\Admin\PycharmProjects\AirlineDataManagement\processedfiles\airline")

#===============================================================================================================================================

    # Q1 In any of your input file if you are getting \N or null values in your column and that column
    # is of string type then put default value as "(unknown)" and if column is of type integer then put -1
    #using data frame
    # resultdf=airlinedf.replace(["\\N"],["(unknown)"],["Alias","IATA","ICAO","callsign","country"])\
    #                   .fillna("(unknown)",("Alias","IATA","ICAO","callsign","country"))

    # resultdf.show()

    #using sql

#================================================================================================================================================
    #Q2.find the country name which is having both airlines and airport
    dataschema2 = StructType([StructField("Airport_ID", IntegerType()),
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

    airportdf = spark.read.csv(r"D:\Akshay\pyspark\airport.csv", schema=dataschema2)
    # airportdf.printSchema()
    # airportdf.show()

    #using DataFrame
    #===============================================================
    # country_having_airport_airline = airportdf.join(airlinedf,on="country",how="inner")\
    #                                 .select("Country").distinct()
    # country_having_airport_airline.show()
    # print(country_having_airport_airline.count())

    #using SQL
    # #=================================================================
    # airportdf.createOrReplaceTempView("airportdata")
    # airlinedf.createOrReplaceTempView("airlinedata")
    # spark.sql("select distinct(country)  from airportdata join airlinedata using(Country)").show()
    # spark.sql("select count(distinct(country))  from airportdata join airlinedata using(Country)").show()


#=================================================================================================================================================

    #Q3A  get the airlines details like name, id which is has taken takeoff more than 3 times
    # from same airport
#==============================================================================================================================
    #using SQL

    routesdf = spark.read.parquet(r"D:\Akshay\pyspark\routes.snappy.parquet")
    # routesdf.printSchema()
    # routesdf.groupBy().count().show()

    # routesdf.createOrReplaceTempView("routes")
    #
    # airlinedf.createOrReplaceTempView("airline")
    #
    #
    # sqlresult=spark.sql("select name,Airline_id "
    #                     "from airline"
    #                     " where Airline_id in \
    #                                 (select airline_id from routes "
    #                                     "group by airline_id \
    #                                     having count(src_airport_id)>3 ) "
    #                     "group by name,Airline_id order by Airline_id")
    # #
    # sqlresult.show()
    # sqlresult.groupBy().count().show()
    #========================
    # RESULT = 523 AIRPORTS |
    #========================

    # #=============================================================================================================================================

    #using DataFrame


    # renamedairlinedf=airlinedf.withColumnRenamed("Airline_id","airline_id")
    # airlines_takeoff_df=renamedairlinedf.join(routesdf,on="airline_id",how="inner").select("name","airline_id").groupBy("name","airline_id").count()
    #
    # airlines_takeoff_df.where(col("count")>3).orderBy("airline_id").show()
    # airlines_takeoff_df.where(col("count")>3).orderBy("airline_id").groupBy().count().show()
    # #========================
    # RESULT = 523 AIRPORTS |
    #========================
#==============================================================================================================================================================

    #Q3B.  get airport details which has minimum number of takeoffs and landing.

#====================================================================================================================================================

    # =====================
    # USING SQL           |
    # =====================
    #minimum takeoffs
    routesdf.createOrReplaceTempView("routes")
    airportdf.createOrReplaceTempView("airport")
    sqlmintakeoff=spark.sql("select * from airport where Airport_ID in (with cte as ( select src_airport_id,count(src_airport_id) as counts from routes "
              "group by src_airport_id) "
              "select src_airport_id from cte "
              "where counts = (select min(counts) from cte))"
              )
    # sqlmintakeoff.show()
    # sqlmintakeoff.groupBy().count().show()
    #=============================================
    # minimun landing
    sqlminlanding = spark.sql(
        "select * from airport where Airport_ID in (with bte as ( select dest_airport_id,count(dest_airport_id) as counts from routes "
        "group by dest_airport_id) "
        "select dest_airport_id from bte "
        "where counts = (select min(counts) from bte))"
        )
    # sqlminlanding.show()
    # sqlminlanding.groupBy().count().show()
    #==============================================
    #airports having minimub takeoffs and landing
    # sqlmintakland=sqlmintakeoff.join(sqlminlanding,on="Airport_ID",how="inner")
    # sqlmintakland.show()
    # sqlmintakland.groupBy().count().show()


    #========================
    # RESULT = 596 AIRPORTS |
    #========================

    # =====================
    # USING DATA FRAME    |
    # =====================
    # Q3B.  get airport details which has minimum number of takeoffs and landing.
    # min_ = routesdf.select("src_airport_id").where("src_airport_id != '0' ").groupBy("src_airport_id").count().agg( min('count') ).head(1)[0]['min(count)']
    #
    # src=routesdf.select("src_airport_id").where("src_airport_id != '0' ").groupBy("src_airport_id").count()
    # src =   src.filter( src['count'] == min_  ).drop("count")
    # dest=routesdf.select("dest_airport_id").where("dest_airport_id != '0' ").groupBy("dest_airport_id").count()
    # dest =   dest.filter( dest['count'] == min_  ).drop("count")
    #
    # conn = src.join(dest,on=src['src_airport_id'] == dest['dest_airport_id'] ).drop("src_airport_id")
    #
    # airportdf.join(conn,on= conn["dest_airport_id"] == airportdf["Airport_ID"]).groupBy().count().show()


    #========================
    # RESULT = 568 AIRPORTS |
    #========================
#==============================================================================================================================================
    #Q4. get airport details which is having maximum number of takeoff and landing.

#==============================================================================================================================================

    #using SQL
    #max takeoffs
    # routesdf.createOrReplaceTempView("routes")
    # airportdf.createOrReplaceTempView("airport")
    # sqlmaxtakeoffs=spark.sql("select * from airport where Airport_ID in (with maxtakeoff as "
    #           "(select src_airport_id, count(src_airport_id) as counts from routes group by src_airport_id )"
    #           "select src_airport_id from maxtakeoff where counts=(select max(counts) from maxtakeoff))")
    # sqlmaxtakeoffs.createOrReplaceTempView("sqlmaxtakeoffs")
    # # sqlmaxtakeoffs.groupBy().count().show()

    #maxlanding
    # sqlmaxlanding=spark.sql("select * from airport where Airport_ID in (with maxlanding as "
    #                         "(select dest_airport_id,count(dest_airport_id) counts from routes group by dest_airport_id)"
    #                         "select dest_airport_id from maxlanding where counts= (select max(counts) from maxlanding))")

    # sqlmaxlanding.createOrReplaceTempView('sqlmaxlanding')
    # sqlmaxlanding.groupBy().count().show()

    #max landing and max takeoff
    # spark.sql("select * from sqlmaxtakeoffs join sqlmaxlanding using(Airport_ID)").show()
    # spark.sql("select * from sqlmaxtakeoffs join sqlmaxlanding using(Airport_ID)").groupBy().count().show()
    #========================
    # RESULT = 1 AIRPORT    |
    #========================
#=============================================================================
    # =====================
    # USING DATA FRAME    |
    # =====================
    # max_ = routesdf.select("src_airport_id").where("src_airport_id != '0' ").groupBy("src_airport_id").count().agg( max('count') ).head(1)[0]['max(count)']
    # # print(max_)
    # src=routesdf.select("src_airport_id").where("src_airport_id != '0' ").groupBy("src_airport_id").count()
    # src =   src.filter( src['count'] == max_  ).drop("count")
    # max_des=routesdf.select("dest_airport_id").where("dest_airport_id != '0' ").groupBy("dest_airport_id").count().agg( max('count') ).head(1)[0]['max(count)']
    # dest=routesdf.select("dest_airport_id").where("dest_airport_id != '0' ").groupBy("dest_airport_id").count()
    # dest =   dest.filter( dest['count'] == max_des  ).drop("count")
    #
    # conn = src.join(dest,on=src['src_airport_id'] == dest['dest_airport_id'] ).drop("src_airport_id")
    #
    # airportdf.join(conn,on= conn["dest_airport_id"] == airportdf["Airport_ID"]).show()
    # airportdf.join(conn,on= conn["dest_airport_id"] == airportdf["Airport_ID"]).groupBy().count().show()
    #========================
    # RESULT = 1 AIRPORT    |
    #========================

#===============================================================================================================================================

#Q5. Get the airline details, which is having direct flights. details like airline id, name, source airport name, and destination airport name
    # =====================
    # using SQL            |
    # =====================
    #
    #=======================
    routesdf.createOrReplaceTempView("routes")
    directroutesdf=spark.sql("select distinct(airline_id),src_airport,dest_airport  from routes where stops==0")
    directroutesdf1=directroutesdf.withColumnRenamed("airline_id","Airline_id")
    directroutesdf1.createOrReplaceTempView("directroutes")
    airlinedf.createOrReplaceTempView("airline")

    sqldirectroutes=spark.sql("select directroutes.airline_id, airline.name, directroutes.src_airport, directroutes.dest_airport from directroutes  join"
                                 " airline  using (Airline_id) order by name")
    sqldirectroutes.show()
    sqldirectroutes.groupBy().count().show()
    #========================
    # RESULT = 67173        |
    #========================
#========================================================================================================================================
    # =====================
    # USING DATA FRAME    |
    # =====================
    dfdirectroutes=routesdf.join(airlinedf,airlinedf["Airline_id"]==routesdf["airline_id"]).select(routesdf["airline_id"],"name","src_airport","dest_airport").\
        where(col("stops")==0).distinct()
    dfdirectroutes.orderBy("name").show()
    dfdirectroutes.groupBy().count().show()
    #========================
    # RESULT = 67173        |
    #========================

