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
        "paths": ["s3://rawgpc9/15-01-2022/olist_closed_deals_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("mql_id", "string", "mql_id", "string"),
        ("seller_id", "string", "seller_id", "string"),
        ("sdr_id", "string", "sdr_id", "string"),
        ("sr_id", "string", "sr_id", "string"),
        ("won_date", "string", "won_date", "timestamp"),
        ("business_segment", "string", "business_segment", "string"),
        ("lead_type", "string", "lead_type", "string"),
        ("lead_behaviour_profile", "string", "lead_behaviour_profile", "string"),
        ("business_type", "string", "business_type", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673968110296 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673968110296",
)

# Script generated for node Amazon S3
AmazonS3_node1674075211411 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Closed-Deals/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075211411",
)
AmazonS3_node1674075211411.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-closed-deals"
)
AmazonS3_node1674075211411.setFormat("glueparquet")
AmazonS3_node1674075211411.writeFrame(DropDuplicates_node1673968110296)
job.commit()
