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
    table_name="processed-closed-deals",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("mql_id", "string", "mql_id", "string"),
        ("seller_id", "string", "seller_id", "string"),
        ("sdr_id", "string", "sdr_id", "string"),
        ("sr_id", "string", "sr_id", "string"),
        ("won_date", "timestamp", "won_date", "timestamp"),
        ("business_segment", "string", "business_segment", "string"),
        ("lead_type", "string", "lead_type", "string"),
        ("lead_behaviour_profile", "string", "lead_behaviour_profile", "string"),
        ("business_type", "string", "business_type", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Redshift Cluster
RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="redshiftdb",
    table_name="olistdb_public_closed_deals",
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="RedshiftCluster_node3",
)

job.commit()
