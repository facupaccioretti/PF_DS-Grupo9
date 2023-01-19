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
AmazonS3_node1674082871720 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://rawgpc9/15-01-2022/olist_order_reviews_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1674082871720",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673972319516 = DynamicFrame.fromDF(
    AmazonS3_node1674082871720.toDF().dropDuplicates(["review_id"]),
    glueContext,
    "DropDuplicates_node1673972319516",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1674055971320 = DynamicFrame.fromDF(
    DropDuplicates_node1673972319516.toDF().dropDuplicates(["order_id"]),
    glueContext,
    "DropDuplicates_node1674055971320",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropDuplicates_node1674055971320,
    mappings=[
        ("review_id", "string", "review_id", "string"),
        ("order_id", "string", "order_id", "string"),
        ("review_score", "string", "review_score", "long"),
        ("review_comment_title", "string", "review_comment_title", "string"),
        ("review_comment_message", "string", "review_comment_message", "string"),
        ("review_creation_date", "string", "review_creation_date", "timestamp"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon S3
AmazonS3_node1674075000633 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Orders-Reviews/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075000633",
)
AmazonS3_node1674075000633.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-orders-reviews"
)
AmazonS3_node1674075000633.setFormat("glueparquet")
AmazonS3_node1674075000633.writeFrame(ApplyMapping_node2)
job.commit()
