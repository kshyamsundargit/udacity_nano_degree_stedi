import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1749388404550 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1749388404550")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1749388720709 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1749388720709")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT ct.* 
FROM customer_trusted ct
JOIN accelerometer_trusted at
ON ct.email = at.user;
'''
SQLQuery_node1749388757285 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customer_trusted_node1749388404550, "accelerometer_trusted":accelerometer_trusted_node1749388720709}, transformation_ctx = "SQLQuery_node1749388757285")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749388757285, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749388213528", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1749388807166 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1749388757285, connection_type="s3", format="json", connection_options={"path": "s3://kss-spark-project/customer/curated/", "partitionKeys": []}, transformation_ctx="customer_curated_node1749388807166")

job.commit()