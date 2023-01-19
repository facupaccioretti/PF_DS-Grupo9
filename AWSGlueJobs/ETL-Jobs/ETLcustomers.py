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
        "customer_city",
        F.translate(
            "customer_city",
            "ãäöüẞáäčďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ",
            "aaousaacdeeillnoorstuuyzAOUSAACDEEILLNOORSTUUYZ",
        ),
    )

    dyf = DynamicFrame.fromDF(df, glueContext, "Customers")

    return DynamicFrameCollection({"CustomTransform0": dyf}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1674083141696 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://rawgpc9/15-01-2022/olist_customers_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1674083141696",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=AmazonS3_node1674083141696,
    mappings=[
        ("customer_id", "string", "customer_id", "string"),
        ("customer_zip_code_prefix", "string", "customer_zip_code_prefix", "int"),
        ("customer_city", "string", "customer_city", "string"),
        ("customer_state", "string", "customer_state", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673970479045 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673970479045",
)

# Script generated for node Custom Transform
CustomTransform_node1674050306671 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"DropDuplicates_node1673970479045": DropDuplicates_node1673970479045},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1674050310965 = SelectFromCollection.apply(
    dfc=CustomTransform_node1674050306671,
    key=list(CustomTransform_node1674050306671.keys())[0],
    transformation_ctx="SelectFromCollection_node1674050310965",
)

# Script generated for node Amazon S3
AmazonS3_node1674075264618 = glueContext.getSink(
    path="s3://transformedolistdata/Processed-Customers/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075264618",
)
AmazonS3_node1674075264618.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-customers"
)
AmazonS3_node1674075264618.setFormat("glueparquet")
AmazonS3_node1674075264618.writeFrame(SelectFromCollection_node1674050310965)
job.commit()
