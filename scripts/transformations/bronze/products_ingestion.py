import dlt

products_rules = {
    'rule1' : 'product_id is not null',
    'rule2' : 'price>=0'

}
# ingesting products
@dlt.table(
    name = 'products_stg'
)
@dlt.expect_all_or_drop(products_rules)
def products_stg():
    df = spark.readStream.table('dltrahul.source.products')
    return df
