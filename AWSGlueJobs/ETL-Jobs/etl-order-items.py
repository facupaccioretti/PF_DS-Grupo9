import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://rawgpc9/15-01-2022/olist_order_items_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673974035351 = DynamicFrame.fromDF(
    S3bucket_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673974035351",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropDuplicates_node1673974035351,
    mappings=[
        ("order_id", "string", "order_id", "string"),
        ("order_item_id", "string", "order_item_id", "string"),
        ("product_id", "string", "product_id", "string"),
        ("seller_id", "string", "seller_id", "string"),
        ("shipping_limit_date", "string", "shipping_limit_date", "timestamp"),
        ("price", "string", "price", "float"),
        ("freight_value", "string", "freight_value", "float"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon S3
AmazonS3_node1674074897454 = glueContext.getSink(
    path="s3://transformedolistdata/Proccessed-Order-Items/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674074897454",
)
AmazonS3_node1674074897454.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-order-items"
)
AmazonS3_node1674074897454.setFormat("glueparquet")
AmazonS3_node1674074897454.writeFrame(ApplyMapping_node2)
job.commit()
