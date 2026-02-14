from word2number import w2n
from pyspark import pipelines as dp
from pyspark.sql.functions import *

#Python to UDF
def word_to_num_logic(value):
    if value is None:
        return None
    try:
        return int(value)
    except:
        try:
            return w2n.word_to_num(str(value).lower())
        except:
            return None
        
convert_age_udf = udf(word_to_num_logic)

@dp.materialized_view(
    name="silver_staff_dlt2",
    comment="Standardized staff data"
)
def silver_staff_dlt1():
    return (
        spark.read.table("bronze_staff_data1")
        .select(col("shipment_id").cast("bigint"),
                convert_age_udf(col("age")).alias("age_clean"),
                lower("role").alias("role"),
                initcap(col("hub_location")).alias("origin_hub_city"),
                current_timestamp().alias("load_dt"),
                concat_ws(" ",col("first_name"),col("last_name")).alias("staff_full_name"),
                initcap(col("hub_location")).alias("hub_location")
                )
        )
    
@dp.materialized_view(
    name="silver_geotag_dlt2",
    comment="Cleaned geotag data",
    table_properties={"quality": "silver"}
)
def silver_geotag_dlt2():
    return (
        spark.read.table("bronze_geotag_data1")
        .select(
            initcap(col("city_name")).alias("city_name"),
            initcap(col("country")).alias("masked_hub_location"),
            col("latitude"),
            col("longitude")
        )
        .distinct()
    )

@dp.materialized_view(
    name="silver_shipments_dlt2",
    comment="Enriched and split shipments data",
    table_properties={"quality": "silver"}
)
def ui():
    ship_date_col = to_date(col("shipment_date"), "yy-MM-dd")
    
    return (
        spark.read.table("bronze_shipments_data1")
        .withColumn("domain", lit("Logistics"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("is_expedited_flag_initial", lit(False).cast("boolean"))
        .withColumn("shipment_date_clean", ship_date_col)
        .withColumn("shipment_cost_clean", round(col("shipment_cost"), 2))
        .withColumn("shipment_weight_clean", col("shipment_weight_kg").cast("double"))
        .withColumn("route_segment", concat_ws("-", col("source_city"), col("destination_city")))
        .withColumn("vehicle_identifier", concat_ws("_", col("vehicle_type"), col("shipment_id")))
        .withColumn("shipment_year", year(ship_date_col))
        .withColumn("shipment_month", month(ship_date_col))
        .withColumn("is_weekend", 
            when(dayofweek(ship_date_col).isin([1, 7]), True)
            .otherwise(False)
        )
        .withColumn("is_expedited", 
            when(col("shipment_status").isin(["IN_TRANSIT", "DELIVERED"]), True)
            .otherwise(False)
        )
        .withColumn("cost_per_kg", round(col("shipment_cost") / col("shipment_weight_kg"), 2))
        .withColumn("tax_amount", round(col("shipment_cost") * 0.18, 2))
        .withColumn("days_since_shipment", datediff(current_date(), ship_date_col))
        .withColumn("is_high_value", 
            when(col("shipment_cost") > 50000, True)
            .otherwise(False))
        .withColumn("order_prefix", substring(col("order_id"), 1, 3))
        .withColumn("order_sequence", substring(col("order_id"), 4, 10))
        .withColumn("ship_day", dayofmonth(ship_date_col))
        .withColumn("route_lane", concat_ws("->", col("source_city"), col("destination_city")))
    )