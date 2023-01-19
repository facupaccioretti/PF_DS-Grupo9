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
AmazonS3_node1673972231783 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://rawgpc9/15-01-2022/olist_marketing_qualified_leads_dataset.csv"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1673972231783",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673972091835 = DynamicFrame.fromDF(
    AmazonS3_node1673972231783.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673972091835",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropDuplicates_node1673972091835,
    mappings=[
        ("mql_id", "string", "mql_id", "string"),
        ("first_contact_date", "string", "first_contact_date", "timestamp"),
        ("landing_page_id", "string", "landing_page_id", "string"),
        ("origin", "string", "origin", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon S3
AmazonS3_node1674075080358 = glueContext.getSink(
    path="s3://transformedolistdata/Proccessed-MQL/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075080358",
)
AmazonS3_node1674075080358.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-mql"
)
AmazonS3_node1674075080358.setFormat("glueparquet")
AmazonS3_node1674075080358.writeFrame(ApplyMapping_node2)
job.commit()
