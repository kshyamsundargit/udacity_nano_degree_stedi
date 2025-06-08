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

# Script generated for node step_trainer_landing
step_trainer_landing_node1749389857066 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/step-trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1749389857066")

# Script generated for node customer_curated
customer_curated_node1749389814666 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://kss-spark-project/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1749389814666")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT stl.*
FROM customer_curated cc 
JOIN step_trainer_landing stl
ON cc.serialnumber = stl.serialnumber;
'''
SQLQuery_node1749389890249 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customer_curated_node1749389814666, "step_trainer_landing":step_trainer_landing_node1749389857066}, transformation_ctx = "SQLQuery_node1749389890249")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749389890249, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749389768709", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1749389970865 = glueContext.getSink(path="s3://kss-spark-project/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1749389970865")
step_trainer_trusted_node1749389970865.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1749389970865.setFormat("json")
step_trainer_trusted_node1749389970865.writeFrame(SQLQuery_node1749389890249)
job.commit()