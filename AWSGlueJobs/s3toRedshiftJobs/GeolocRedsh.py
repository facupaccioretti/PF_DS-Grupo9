import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="olist-processed",
    table_name="processed-geolocation",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("geolocation_lat", "decimal", "geolocation_lat", "decimal"),
        ("geolocation_state", "string", "geolocation_state", "string"),
        ("geolocation_city", "string", "geolocation_city", "string"),
        ("geolocation_lng", "decimal", "geolocation_lng", "decimal"),
        ("geolocation_zip_code_prefix", "int", "geolocation_zip_code_prefix", "int"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="redshiftdb",
    table_name="olistdb_public_geolocation",
    redshift_tmp_dir=args["TempDir"],
    additional_options={"aws_iam_role": "arn:aws:iam::904328944752:role/RedshiftFull"},
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
