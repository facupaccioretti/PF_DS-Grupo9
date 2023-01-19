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
    table_name="processed-orders",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("order_id", "string", "order_id", "string"),
        ("customer_id", "string", "customer_id", "string"),
        ("order_status", "string", "order_status", "string"),
        (
            "order_purchase_timestamp",
            "timestamp",
            "order_purchase_timestamp",
            "timestamp",
        ),
        ("order_approved_at", "timestamp", "order_approved_at", "timestamp"),
        (
            "order_delivered_carrier_date",
            "timestamp",
            "order_delivered_carrier_date",
            "timestamp",
        ),
        (
            "order_delivered_customer_date",
            "timestamp",
            "order_delivered_customer_date",
            "timestamp",
        ),
        (
            "order_estimated_delivery_date",
            "timestamp",
            "order_estimated_delivery_date",
            "timestamp",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="redshiftdb",
    table_name="olistdb_public_orders",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
