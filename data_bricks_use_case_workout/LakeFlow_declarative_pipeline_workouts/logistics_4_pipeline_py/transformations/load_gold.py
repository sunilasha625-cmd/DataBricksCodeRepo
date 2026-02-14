from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name="gold_staff_geo_enriched_dlt2",
    comment="Staff enriched with Geo Location data",
    table_properties={"quality": "gold"}
)
def gold_staff_geo_enriched_dlt2():
    # Read the upstream silver tables
    df_staff = spark.read.table("silver_staff_dlt2")
    df_geo = spark.read.table("silver_geotag_dlt2")
    
    return (
        df_staff.join(
            df_geo, 
            df_staff.hub_location == df_geo.city_name, 
            "inner"
        )
        .select(
            df_staff["*"],
            df_geo["latitude"],
            df_geo["longitude"]
        )
    )

@dp.materialized_view(
    name="gold_shipment_stats1",
    comment="Aggregated Shipment statistics by Source City",
    table_properties={"quality": "gold"}
)
def gold_shipment_stats1():
    return (
        spark.read.table("silver_shipments_dlt2")
        .groupBy("source_city")
        .agg(
            sum("shipment_cost_clean").alias("total_cost"),
            count("shipment_id").alias("total_shipments"),
            avg("shipment_weight_clean").alias("avg_weight")
        )
    )
