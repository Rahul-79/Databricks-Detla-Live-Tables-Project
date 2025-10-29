import dlt
from pyspark.sql.functions import *

@dlt.view(
    name = 'customers_enr_view'
)
def customers_stg_transform():
    df = spark.readStream.table('customers_stg')
    df = df.withColumn('customer_name',upper(col('customer_name')))
    return df

dlt.create_streaming_table(
    name = 'customers_enriched'
)
dlt.create_auto_cdc_flow(
    target = "customers_enriched",
    source = "customers_enr_view",
    keys = ["customer_id"],
    sequence_by = 'last_updated',
    stored_as_scd_type = 1
)

