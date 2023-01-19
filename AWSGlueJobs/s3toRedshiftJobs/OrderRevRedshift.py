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

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="olist-processed",
    table_name="processed-orders-reviews",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("review_id", "string", "review_id", "string"),
        ("order_id", "string", "order_id", "string"),
        ("review_score", "long", "review_score", "bigint"),
        ("review_comment_title", "string", "review_comment_title", "string"),
        ("review_comment_message", "string", "review_comment_message", "string"),
        ("review_creation_date", "timestamp", "review_creation_date", "timestamp"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="redshiftdb",
    table_name="olistdb_public_order_reviews",
    redshift_tmp_dir=args["TempDir"],
    additional_options={"aws_iam_role": "arn:aws:iam::904328944752:role/RedshiftFull"},
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
