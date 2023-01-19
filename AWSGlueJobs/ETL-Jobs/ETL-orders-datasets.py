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
        "paths": ["s3://rawgpc9/15-01-2022/olist_orders_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("order_id", "string", "order_id", "string"),
        ("customer_id", "string", "customer_id", "string"),
        ("order_status", "string", "order_status", "string"),
        ("order_purchase_timestamp", "string", "order_purchase_timestamp", "timestamp"),
        ("order_approved_at", "string", "order_approved_at", "timestamp"),
        (
            "order_delivered_carrier_date",
            "string",
            "order_delivered_carrier_date",
            "timestamp",
        ),
        (
            "order_delivered_customer_date",
            "string",
            "order_delivered_customer_date",
            "timestamp",
        ),
        (
            "order_estimated_delivery_date",
            "string",
            "order_estimated_delivery_date",
            "timestamp",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673972502413 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673972502413",
)

# Script generated for node Amazon S3
AmazonS3_node1674075145097 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Orders/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075145097",
)
AmazonS3_node1674075145097.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-orders"
)
AmazonS3_node1674075145097.setFormat("glueparquet")
AmazonS3_node1674075145097.writeFrame(DropDuplicates_node1673972502413)
job.commit()
