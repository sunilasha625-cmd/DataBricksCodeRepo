from pyspark import pipelines
#load_data_bronze_imp_dp().write.saveAsTable("lakehousecat.default.shipment1_bronze")
@pipelines.table(name="etl_practice.etl_data.shipment1_bronze1") #it becomes declarative by specifying decorator on top of the function
def load_data_bronze_imp_dp(): #Imperative program
    df1=spark.read.table("gcp_mysql_fc1.practice2.shipments") #foreign catalog external DB source
    df2=df1.where("city is NULL")
    return df2

#Few important understanding we need to get out of this program (most of our DP learning will be over if you do this..)
#1. How to write a declarative program rather than imperative - We used pipelines and decorator
#2. How to handle batch data ingestion from the source - 
#  a. Source table will be collected completely with inserted/updated/deleted data
#->This means:
#Every time the pipeline runs:
#It reads the entire source table
#It sees:
#New rows (Inserted)
#Changed rows (Updated)
#Missing rows (Deleted)
#Even if only 2 rows changed…
#Spark still checks the complete table.

#  b. Target table (materialized view) will be updated with only inserted/updated/deleted data (We will not be updating the target table with all the data from source table)
#Even though Spark reads the full source…
#👉 It does NOT rewrite the entire target table
#Instead it:
#Inserts only new rows
#Updates only changed rows
#Deletes only removed rows
#This is called:
#✅ Incremental Processing