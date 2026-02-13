from pyspark import pipelines as dp

@dp.table(name="etl_practice.etl_data.drugs_stream_bronze_table1")
def streaming_load():
  df1=spark.readStream.table("lakehousecat.deltadb.drugstb1")
  return df1
#Few important understanding we need to get out of this program (most of our DP learning will be over if you do this..)
#1. How to write a declarative program rather than imperative - We used pipelines and decorator
#2. How to handle streaming data ingestion (only inserted) from the source (fact table/transactions/events tables) - 
#  a. Source table will be collected incremental with only inserted data (if i use readStream.table function)
#  b. Target table (stream table) will allow insert with inserted/newly added data from the source (it will not engage updated/delete in the target)
