from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(name="bronze_staff_data1")
def bronze_staff_data():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format","csv")
            .option("inferColumnTypes","True")
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/prodcatalog1/logistics1/datalake/staff_new/"))

@dp.table(name="bronze_geotag_data1")
def bronze_geotag_data():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("inferColumnTypes", "true")
        .load("/Volumes/prodcatalog1/logistics1/datalake/geotag_new/")
    )

@dp.table(name="bronze_shipments_data1")
def bronze_shipments_data():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("inferColumnTypes", "true")
            .option("multiLine", "true")
            .load("/Volumes/prodcatalog1/logistics1/datalake/shipments_new/")
            .select(
                col("shipment_id").cast("string").alias("shipment_id"),
                col("order_id").cast("string").alias("order_id"),
                "source_city",
                "destination_city",
                "shipment_status",
                "cargo_type",
                "vehicle_type",
                "payment_mode",
                "shipment_weight_kg",
                "shipment_cost",
                "shipment_date"
            )
    )
