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
        "paths": ["s3://rawgpc9/15-01-2022/olist_products_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1673989230709 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://rawgpc9/15-01-2022/product_category_name_translation.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1673989230709",
)

# Script generated for node Schema Matcher for Union
SchemaMatcherforUnion_node1673989322390 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("product_id", "string", "product_id", "string"),
        ("product_category_name", "string", "product_category_name", "string"),
        ("product_weight_g", "string", "product_weight_g", "float"),
        ("product_length_cm", "string", "product_length_cm", "float"),
        ("product_height_cm", "string", "product_height_cm", "float"),
        ("product_width_cm", "string", "product_width_cm", "float"),
    ],
    transformation_ctx="SchemaMatcherforUnion_node1673989322390",
)

# Script generated for node Schema Matcher for Union
SchemaMatcherforUnion_node1673989322385 = ApplyMapping.apply(
    frame=AmazonS3_node1673989230709,
    mappings=[
        ("product_category_name", "string", "product_category_name", "string"),
        (
            "product_category_name_english",
            "string",
            "product_category_name_english",
            "string",
        ),
    ],
    transformation_ctx="SchemaMatcherforUnion_node1673989322385",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673973280149 = DynamicFrame.fromDF(
    SchemaMatcherforUnion_node1673989322390.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673973280149",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673989522943 = DynamicFrame.fromDF(
    SchemaMatcherforUnion_node1673989322385.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673989522943",
)

# Script generated for node Join
DropDuplicates_node1673989522943DF = DropDuplicates_node1673989522943.toDF()
DropDuplicates_node1673973280149DF = DropDuplicates_node1673973280149.toDF()
Join_node1673989564221 = DynamicFrame.fromDF(
    DropDuplicates_node1673989522943DF.join(
        DropDuplicates_node1673973280149DF,
        (
            DropDuplicates_node1673989522943DF["product_category_name"]
            == DropDuplicates_node1673973280149DF["product_category_name"]
        ),
        "right",
    ),
    glueContext,
    "Join_node1673989564221",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1674070398920 = ApplyMapping.apply(
    frame=Join_node1673989564221,
    mappings=[
        (
            "product_category_name_english",
            "string",
            "product_category_name_english",
            "string",
        ),
        ("product_id", "string", "product_id", "string"),
        ("product_category_name", "string", "product_category_name", "string"),
        ("product_weight_g", "float", "product_weight_g", "float"),
        ("product_length_cm", "float", "product_length_cm", "float"),
        ("product_height_cm", "float", "product_height_cm", "float"),
        ("product_width_cm", "float", "product_width_cm", "float"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1674070398920",
)

# Script generated for node Amazon S3
AmazonS3_node1674074942511 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Product/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674074942511",
)
AmazonS3_node1674074942511.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-product"
)
AmazonS3_node1674074942511.setFormat("glueparquet")
AmazonS3_node1674074942511.writeFrame(ChangeSchemaApplyMapping_node1674070398920)
job.commit()
