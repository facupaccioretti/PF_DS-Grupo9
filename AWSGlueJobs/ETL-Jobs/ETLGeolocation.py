import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df.withColumn(
        "geolocation_city",
        F.translate(
            "geolocation_city",
            "ãäöüẞáäčďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ",
            "aaousaacdeeillnoorstuuyzAOUSAACDEEILLNOORSTUUYZ",
        ),
    )

    dyf = DynamicFrame.fromDF(df, glueContext, "Geolocation")

    return DynamicFrameCollection({"CustomTransform0": dyf}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://rawgpc9/15-01-2022/olist_geolocation_dataset.csv"],
        "recurse": True,
    },
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1673970729332 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("geolocation_zip_code_prefix", "string", "geolocation_zip_code_prefix", "int"),
        ("geolocation_lat", "string", "geolocation_lat", "double"),
        ("geolocation_lng", "string", "geolocation_lng", "double"),
        ("geolocation_city", "string", "geolocation_city", "string"),
        ("geolocation_state", "string", "geolocation_state", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1673970729332",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673970748071 = DynamicFrame.fromDF(
    ChangeSchemaApplyMapping_node1673970729332.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1673970748071",
)

# Script generated for node Custom Transform
CustomTransform_node1673990550488 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"DropDuplicates_node1673970748071": DropDuplicates_node1673970748071},
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1673997713218 = SelectFromCollection.apply(
    dfc=CustomTransform_node1673990550488,
    key=list(CustomTransform_node1673990550488.keys())[0],
    transformation_ctx="SelectFromCollection_node1673997713218",
)

# Script generated for node Filter
Filter_node1674145154239 = Filter.apply(
    frame=SelectFromCollection_node1673997713218,
    f=lambda row: (
        row["geolocation_lat"] <= 5.27438888
        and row["geolocation_lat"] >= -33.75116944
        and row["geolocation_lng"] >= -73.98283055
        and row["geolocation_lng"] <= -34.79314722
    ),
    transformation_ctx="Filter_node1674145154239",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1674145298771 = ApplyMapping.apply(
    frame=Filter_node1674145154239,
    mappings=[
        ("geolocation_zip_code_prefix", "int", "geolocation_zip_code_prefix", "int"),
        ("geolocation_lat", "double", "geolocation_lat", "decimal"),
        ("geolocation_lng", "double", "geolocation_lng", "decimal"),
        ("geolocation_city", "string", "geolocation_city", "string"),
        ("geolocation_state", "string", "geolocation_state", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1674145298771",
)

# Script generated for node Amazon S3
AmazonS3_node1674075314226 = glueContext.getSink(
    path="s3://transformedolistdata/Process-Geolocation/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1674075314226",
)
AmazonS3_node1674075314226.setCatalogInfo(
    catalogDatabase="olist-processed", catalogTableName="processed-geolocation"
)
AmazonS3_node1674075314226.setFormat("glueparquet")
AmazonS3_node1674075314226.writeFrame(ChangeSchemaApplyMapping_node1674145298771)
job.commit()
