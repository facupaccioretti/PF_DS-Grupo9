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

# Script generated for node Amazon S3
AmazonS3_node1674082640531 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://rawgpc9/15-01-2022/olist_order_payments_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1674082640531",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673971434270 = DynamicFrame.fromDF(
    AmazonS3_node1674082640531.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673971434270",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropDuplicates_node1673971434270,
    mappings=[
        ("order_id", "string", "order_id", "string"),
        ("payment_sequential", "string", "payment_sequential", "long"),
        ("payment_type", "string", "payment_type", "string"),
        ("payment_installments", "string", "payment_installments", "long"),
        ("payment_value", "string", "payment_value", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon S3
AmazonS3_node1674075206329 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Order-Payments/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075206329",
)
AmazonS3_node1674075206329.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-order-payments"
)
AmazonS3_node1674075206329.setFormat("glueparquet")
AmazonS3_node1674075206329.writeFrame(ApplyMapping_node2)
job.commit()
