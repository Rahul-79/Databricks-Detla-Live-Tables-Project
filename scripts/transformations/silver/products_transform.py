import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = 'products_enr_view'
)
def sales_stg_transform():
    df = spark.readStream.table('products_stg')
    df = df.withColumn('price',col('price').cast(IntegerType()))
    return df

dlt.create_streaming_table(
    name = 'products_enriched'
)
dlt.create_auto_cdc_flow(
    target = "products_enriched",
    source = "products_enr_view",
    keys = ["product_id"],
    sequence_by = 'last_updated',
    stored_as_scd_type = 1
)
