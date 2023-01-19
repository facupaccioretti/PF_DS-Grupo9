import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df.withColumn(
        "seller_city",
        F.translate(
            "seller_city",
            "ãäöüẞáäčďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ",
            "aaousaacdeeillnoorstuuyzAOUSAACDEEILLNOORSTUUYZ",
        ),
    )

    dyf = DynamicFrame.fromDF(df, glueContext, "Sellers")

    return DynamicFrameCollection({"CustomTransform0": dyf}, glueContext)


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
        "paths": ["s3://rawgpc9/15-01-2022/olist_sellers_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1674074036384 = DynamicFrame.fromDF(
    S3bucket_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1674074036384",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DropDuplicates_node1674074036384,
    mappings=[
        ("seller_id", "string", "seller_id", "string"),
        ("seller_zip_code_prefix", "string", "seller_zip_code_prefix", "long"),
        ("seller_city", "string", "seller_city", "string"),
        ("seller_state", "string", "seller_state", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Custom Transform
CustomTransform_node1674074062565 = MyTransform(
    glueContext,
    DynamicFrameCollection({"ApplyMapping_node2": ApplyMapping_node2}, glueContext),
)

# Script generated for node Select From Collection
SelectFromCollection_node1674074429977 = SelectFromCollection.apply(
    dfc=CustomTransform_node1674074062565,
    key=list(CustomTransform_node1674074062565.keys())[0],
    transformation_ctx="SelectFromCollection_node1674074429977",
)

# Script generated for node Amazon S3
AmazonS3_node1674083362775 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Sellers/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674083362775",
)
AmazonS3_node1674083362775.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-sellers"
)
AmazonS3_node1674083362775.setFormat("glueparquet")
AmazonS3_node1674083362775.writeFrame(SelectFromCollection_node1674074429977)
job.commit()
