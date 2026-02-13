from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="etl_practice.etl_data.drugstbl_medal_bronze3")
def drugstbl_bronze_tbl():
    return spark.read.table("lakehousecat.deltadb.drugstb1")#requirement is to collect ins/upd/deleted data and merge to bronze table
    #return spark.readStream.table("lakehousecat.deltadb.drugstbl")#requirement is to collect only ins data and merge to bronze table
#This is equivalent to spark createOrReplaceTempView("drugstbl_silverview2")
@dp.view(name="drugstbl_silverview2")#We use view very specifically, if we don't need the data to replicated in silver layer also (because in this project no one is going to use this silver data)
@dp.expect_all_or_drop({"rating_is_valid": "rating IN (9,10)"})
def drugstbl_silvernewtbl():
    return spark.read.table("etl_practice.etl_data.drugstbl_medal_bronze3").filter("rating IN (9,10)")

@dp.materialized_view(name="etl_practice.etl_data.drugstbl_medal_gold_mv2")
def drugstbl_medal_gold():
    df = spark.read.table("drugstbl_silverview2")
    return (
        df.groupBy("drugname")
        .agg(F.sum("usefulcount").alias("sum_usefulcount"),
             F.avg("rating").alias("avg_rating")))
